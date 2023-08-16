use crate::endpoint::{
    envelope_in, envelope_out, ChangeClusterRequest, CheckClusterRequest, DeliverMessage,
    EnvelopeIn, EnvelopeOut, ExecuteProposalRequest, ExecuteProposalResponse, LogMessage,
    LogSeverity, StartNodeRequest, StartNodeResponse, StopNodeRequest,
};
use crate::model::{Actor, ActorContext, Severity};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::rc::Weak;
use core::cell::RefCell;
use core::cell::RefMut;
use core::cmp;
use core::mem;
use platform::{Application, Host, MessageEnvelope, PalError};
use prost::Message;
use raft::{eraftpb::Snapshot, storage::MemStorage, Config, RawNode, StateRole};
use slog::{o, Discard, Logger};

struct RaftNode {
    id: u64,
    logger: Logger,
    raft: Option<Box<RawNode<MemStorage>>>,
}

impl Default for RaftNode {
    fn default() -> Self {
        RaftNode {
            id: 0,
            logger: Logger::root(Discard, o!()),
            raft: None,
        }
    }
}

impl RaftNode {
    fn new(node_id: u64, is_leader: bool) -> Result<RaftNode, PalError> {
        let config = Config::new(node_id);

        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 1;
        snapshot.mut_metadata().term = 1;
        snapshot.mut_metadata().mut_conf_state().voters = vec![node_id];

        let storage = MemStorage::new();
        if is_leader {
            storage
                .wl()
                .apply_snapshot(snapshot)
                .map_err(|_e| PalError::Internal)?;
        }

        let logger = Logger::root(Discard, o!());
        let raft =
            Box::new(RawNode::new(&config, storage, &logger).map_err(|_e| PalError::Internal)?);

        Ok(RaftNode {
            id: node_id,
            logger: logger,
            raft: Some(raft),
        })
    }

    fn mut_raft_node(&mut self) -> &mut RawNode<MemStorage> {
        self.raft.as_mut().expect("raft node").as_mut()
    }

    fn raft_node(&self) -> &RawNode<MemStorage> {
        self.raft.as_ref().expect("raft node").as_ref()
    }

    fn is_started(&self) -> bool {
        self.raft.is_some()
    }

    fn is_leader(&self) -> bool {
        self.is_started() && self.raft_node().status().ss.raft_state == StateRole::Leader
    }
}

pub enum DriverState {
    Created,
    Started,
    Stopped,
}

pub struct DriverConfig {
    pub is_leader: bool,
}

struct DriverCore {
    actor: Weak<RefCell<Box<dyn Actor>>>,
    driver_config: DriverConfig,
    driver_state: DriverState,
    messages: Vec<EnvelopeOut>,
    raft_node_id: u64,
    app_config: Vec<u8>,
    prev_instant: u64,
    instant: u64,
    raft_node: RaftNode,
}

impl DriverCore {
    fn new(actor: Weak<RefCell<Box<dyn Actor>>>, driver_config: DriverConfig) -> Self {
        DriverCore {
            actor,
            driver_config,
            driver_state: DriverState::Created,
            messages: Vec::new(),
            raft_node_id: 0,
            app_config: Vec::new(),
            prev_instant: 0,
            instant: 0,
            raft_node: RaftNode {
                ..Default::default()
            },
        }
    }

    fn set_instant(&mut self, instant: u64) {
        self.prev_instant = self.instant;
        self.instant = cmp::max(self.instant, instant);
    }

    fn mut_actor(&mut self) -> Rc<RefCell<Box<dyn Actor>>> {
        self.actor.upgrade().unwrap()
    }

    fn check_started(&self) -> Result<(), PalError> {
        let DriverState::Started = self.driver_state else {
            return Err(PalError::InvalidOperation);
        };

        Ok(())
    }

    fn process_init(&mut self, app_config: Vec<u8>, _app_signing_key: Vec<u8>) {
        self.app_config = app_config;
        self.raft_node_id = 42;
    }

    fn process_start_node(
        &mut self,
        start_node_request: &StartNodeRequest,
        actor_context: Box<dyn ActorContext>,
    ) -> Result<(), PalError> {
        let DriverState::Created = self.driver_state else {
            return Err(PalError::InvalidOperation);
        };

        self.raft_node = RaftNode::new(self.get_id(), start_node_request.is_leader)?;

        let actor = self.mut_actor();
        (*actor.borrow_mut())
            .on_init(actor_context)
            .map_err(|_e| PalError::Internal)?;

        self.messages.push(EnvelopeOut {
            msg: Some(envelope_out::Msg::StartNode(StartNodeResponse {
                node_id: self.get_id(),
            })),
        });

        Ok(())
    }

    fn process_stop_node(&mut self, _stop_node_request: &StopNodeRequest) -> Result<(), PalError> {
        if let DriverState::Stopped = self.driver_state {
            return Ok(());
        }

        let actor = self.mut_actor();
        (*actor.borrow_mut()).on_shutdown();

        Ok(())
    }

    fn process_change_cluster(
        &mut self,
        _change_cluster_request: &ChangeClusterRequest,
    ) -> Result<(), PalError> {
        self.check_started()?;

        Ok(())
    }

    fn process_check_cluster(
        &mut self,
        _check_cluster_request: &CheckClusterRequest,
    ) -> Result<(), PalError> {
        self.check_started()?;

        Ok(())
    }

    fn process_deliver_message(
        &mut self,
        _deliver_message: &DeliverMessage,
    ) -> Result<(), PalError> {
        self.check_started()?;

        Ok(())
    }

    fn process_execute_proposal(
        &mut self,
        execute_proposal_request: &ExecuteProposalRequest,
    ) -> Result<(), PalError> {
        self.check_started()?;

        let actor = self.mut_actor();
        (*actor.borrow_mut())
            .on_process_command(execute_proposal_request.proposal_contents.as_ref())
            .map_err(|_e| PalError::Internal)?;

        Ok(())
    }

    fn process_consensus(&mut self) -> Result<Vec<EnvelopeOut>, PalError> {
        Ok(mem::take(&mut self.messages))
    }

    fn get_id(&self) -> u64 {
        self.raft_node_id
    }

    fn get_instant(&self) -> u64 {
        self.instant
    }

    fn get_config(&self) -> Vec<u8> {
        self.app_config.clone()
    }

    fn is_leader(&self) -> bool {
        self.raft_node.is_leader()
    }

    fn propose_event(&mut self, _event: Vec<u8>) -> Result<(), crate::model::ActorError> {
        todo!()
    }

    fn send_message(&mut self, message: EnvelopeOut) {
        self.messages.push(message);
    }

    fn log_entry(&mut self, severity: Severity, message: String) {
        let log_severity = match severity {
            Severity::Info => LogSeverity::Info,
            Severity::Warning => LogSeverity::Warning,
            Severity::Error => LogSeverity::Error,
        };

        self.send_message(EnvelopeOut {
            msg: Some(envelope_out::Msg::Log(LogMessage {
                severity: log_severity.into(),
                message,
            })),
        });
    }
}

struct DriverContext {
    core: Rc<RefCell<DriverCore>>,
}

impl DriverContext {
    fn new(core: Rc<RefCell<DriverCore>>) -> Self {
        DriverContext { core }
    }
}

impl ActorContext for DriverContext {
    fn get_id(&self) -> u64 {
        self.core.borrow().get_id()
    }

    fn get_instant(&self) -> u64 {
        self.core.borrow().get_instant()
    }

    fn get_config(&self) -> Vec<u8> {
        self.core.borrow().get_config()
    }

    fn is_leader(&self) -> bool {
        self.core.borrow().is_leader()
    }

    fn propose_event(&mut self, event: Vec<u8>) -> Result<(), crate::model::ActorError> {
        self.core.borrow_mut().propose_event(event)
    }

    fn send_message(&mut self, message: Vec<u8>) {
        self.core.borrow_mut().send_message(EnvelopeOut {
            msg: Some(envelope_out::Msg::ExecuteProposal(
                ExecuteProposalResponse {
                    result_contents: message,
                },
            )),
        });
    }

    fn log_entry(&mut self, severity: Severity, message: String) {
        self.core.borrow_mut().log_entry(severity, message);
    }
}

pub struct DriverApplication {
    core: Rc<RefCell<DriverCore>>,
    #[allow(dead_code)]
    actor: Rc<RefCell<Box<dyn Actor>>>,
}

impl DriverApplication {
    pub fn new(config: DriverConfig, actor: Box<dyn Actor>) -> Self {
        let actor = Rc::new(RefCell::new(actor));
        DriverApplication {
            core: Rc::new(RefCell::new(DriverCore::new(Rc::downgrade(&actor), config))),
            actor,
        }
    }

    fn mut_core(&mut self) -> RefMut<'_, DriverCore> {
        self.core.borrow_mut()
    }

    fn send_messages(&mut self, host: &mut impl Host, out_messages: Vec<EnvelopeOut>) {
        let mut serialized_out_messages: Vec<MessageEnvelope> =
            Vec::with_capacity(out_messages.len());
        for out_message in &out_messages {
            serialized_out_messages.push(out_message.encode_to_vec());
        }

        host.send_messages(&serialized_out_messages).unwrap();
    }
}

impl Application for DriverApplication {
    fn receive_messages(
        &mut self,
        host: &mut impl Host,
        instant: u64,
        in_messages: &[MessageEnvelope],
    ) -> Result<(), PalError> {
        self.mut_core().set_instant(instant);

        for serialized_message in in_messages {
            let deserialized_message =
                EnvelopeIn::decode(serialized_message.as_ref()).map_err(|_e| PalError::Decoding)?;
            let message = deserialized_message.msg.ok_or(PalError::UnknownMessage)?;

            match message {
                envelope_in::Msg::StartNode(ref start_node_request) => {
                    let actor_context = Box::new(DriverContext::new(Rc::clone(&self.core)));

                    self.mut_core().process_init(
                        host.get_self_config(),
                        host.get_self_attestation().public_signing_key(),
                    );

                    self.mut_core()
                        .process_start_node(start_node_request, actor_context)
                }
                envelope_in::Msg::StopNode(ref stop_node_request) => {
                    self.mut_core().process_stop_node(stop_node_request)
                }
                envelope_in::Msg::ChangeCluster(ref change_cluster_request) => self
                    .mut_core()
                    .process_change_cluster(change_cluster_request),
                envelope_in::Msg::CheckCluster(ref check_cluster_request) => {
                    self.mut_core().process_check_cluster(check_cluster_request)
                }
                envelope_in::Msg::DeliverMessage(ref deliver_message) => {
                    self.mut_core().process_deliver_message(deliver_message)
                }
                envelope_in::Msg::ExecuteProposal(ref execute_proposal_request) => self
                    .mut_core()
                    .process_execute_proposal(execute_proposal_request),
            }?;
        }

        let out_messages = self.mut_core().process_consensus()?;
        self.send_messages(host, out_messages);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}
