use crate::endpoint::{
    envelope_in, envelope_out, ChangeClusterRequest, ChangeClusterType, CheckClusterRequest,
    CheckClusterResponse, DeliverMessage, EnvelopeIn, EnvelopeOut, ExecuteProposalRequest,
    ExecuteProposalResponse, LogMessage, LogSeverity, StartNodeRequest, StartNodeResponse,
    StopNodeRequest,
};
use crate::model::{Actor, ActorContext, Severity};
use crate::util::log::create_logger;
use crate::util::raft::{
    create_raft_config_change, deserialize_config_change, deserialize_raft_message, get_conf_state,
    serialize_raft_message,
};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::cell::RefMut;
use core::cmp;
use core::mem;
use platform::{Application, Host, MessageEnvelope, PalError};
use prost::Message;
use raft::{
    eraftpb::ConfChangeType as RaftConfigChangeType, eraftpb::ConfState as RaftConfState,
    eraftpb::Entry as RaftEntry, eraftpb::EntryType as RaftEntryType, eraftpb::HardState,
    eraftpb::Message as RaftMessage, eraftpb::Snapshot as RaftSnapshot, storage::MemStorage,
    Config as RaftConfig, RawNode, StateRole as RaftStateRole,
};
use raft::{SoftState, Status};
use slog::Logger;
type RaftNode = RawNode<MemStorage>;

struct DriverContextCore {
    id: u64,
    instant: u64,
    config: Vec<u8>,
    leader: bool,
    proposals: Vec<Vec<u8>>,
    messages: Vec<EnvelopeOut>,
}

impl DriverContextCore {
    fn new() -> DriverContextCore {
        DriverContextCore {
            id: 0,
            instant: 0,
            config: Vec::new(),
            leader: false,
            proposals: Vec::new(),
            messages: Vec::new(),
        }
    }

    fn set_state(&mut self, instant: u64, leader: bool) {
        self.instant = instant;
        self.leader = leader;
    }

    fn set_immutable_state(&mut self, id: u64, config: Vec<u8>) {
        self.id = id;
        self.config = config;
    }

    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_instant(&self) -> u64 {
        self.instant
    }

    fn get_leader(&self) -> bool {
        self.leader
    }

    fn get_config(&self) -> Vec<u8> {
        self.config.clone()
    }

    fn append_proposal(&mut self, proposal: Vec<u8>) {
        self.proposals.push(proposal);
    }

    fn append_message(&mut self, message: EnvelopeOut) {
        self.messages.push(message);
    }

    fn take_outputs(&mut self) -> (Vec<Vec<u8>>, Vec<EnvelopeOut>) {
        (
            mem::take(&mut self.proposals),
            mem::take(&mut self.messages),
        )
    }
}

struct DriverContext {
    core: Rc<RefCell<DriverContextCore>>,
}

impl DriverContext {
    fn new(core: Rc<RefCell<DriverContextCore>>) -> Self {
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
        self.core.borrow().get_leader()
    }

    fn propose_event(&mut self, event: Vec<u8>) -> Result<(), crate::model::ActorError> {
        self.core.borrow_mut().append_proposal(event);

        Ok(())
    }

    fn send_message(&mut self, message: Vec<u8>) {
        self.core.borrow_mut().append_message(EnvelopeOut {
            msg: Some(envelope_out::Msg::ExecuteProposal(
                ExecuteProposalResponse {
                    result_contents: message,
                },
            )),
        });
    }

    fn log_entry(&mut self, severity: Severity, message: String) {
        let log_severity = match severity {
            Severity::Info => LogSeverity::Info,
            Severity::Warning => LogSeverity::Warning,
            Severity::Error => LogSeverity::Error,
        };

        self.core.borrow_mut().append_message(EnvelopeOut {
            msg: Some(envelope_out::Msg::Log(LogMessage {
                severity: log_severity.into(),
                message,
            })),
        });
    }
}

enum DriverState {
    Created,
    Started,
    Stopped,
}

pub struct DriverConfig {
    pub tick_period: u64,
    pub snapshot_count: u64,
}

struct RaftState {
    leader_node_id: u64,
    leader_term: u64,
    committed_cluster_config: Vec<u64>,
    applied_change_id: u64,
    pending_change_id: u64,
}

impl RaftState {
    fn new() -> RaftState {
        RaftState {
            leader_node_id: 0,
            leader_term: 0,
            committed_cluster_config: Vec::new(),
            applied_change_id: 0,
            pending_change_id: 0,
        }
    }
}

pub struct Driver {
    core: Rc<RefCell<DriverContextCore>>,
    actor: Box<dyn Actor>,
    driver_config: DriverConfig,
    driver_state: DriverState,
    messages: Vec<EnvelopeOut>,
    raft_node_id: u64,
    instant: u64,
    tick_instant: u64,
    logger: Logger,
    raft_node: Option<Box<RaftNode>>,
    raft_state: RaftState,
}

impl Driver {
    pub fn new(driver_config: DriverConfig, actor: Box<dyn Actor>) -> Self {
        Driver {
            core: Rc::new(RefCell::new(DriverContextCore::new())),
            actor,
            driver_config,
            driver_state: DriverState::Created,
            messages: Vec::new(),
            raft_node_id: 0,
            instant: 0,
            tick_instant: 0,
            logger: create_logger(),
            raft_node: None,
            raft_state: RaftState::new(),
        }
    }

    fn mut_core(&mut self) -> RefMut<'_, DriverContextCore> {
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

    fn initilize_raft_node(&mut self, is_leader: bool) -> Result<(), PalError> {
        let config = RaftConfig::new(self.get_id());

        let mut snapshot = RaftSnapshot::default();
        snapshot.mut_metadata().index = 1;
        snapshot.mut_metadata().term = 1;
        snapshot.mut_metadata().mut_conf_state().voters = vec![self.get_id()];

        let storage = MemStorage::new();
        if is_leader {
            storage
                .wl()
                .apply_snapshot(snapshot)
                .map_err(|_e| PalError::Internal)?;
        }

        self.raft_node = Some(Box::new(
            RawNode::new(&config, storage, &self.logger).map_err(|_e| PalError::Internal)?,
        ));
        self.tick_instant = self.instant;

        Ok(())
    }

    fn mut_raft_node(&mut self) -> &mut RaftNode {
        self.raft_node.as_mut().expect("initialized").as_mut()
    }

    fn raft_node(&self) -> &RaftNode {
        self.raft_node.as_ref().expect("initialized").as_ref()
    }

    fn check_raft_leadership(&self) -> bool {
        self.raft_node.is_some() && self.raft_node().status().ss.raft_state == RaftStateRole::Leader
    }

    fn make_raft_step(
        &mut self,
        sender_node_id: u64,
        recipient_node_id: u64,
        message_contents: &Vec<u8>,
    ) {
        let message = deserialize_raft_message(message_contents).unwrap();
        if self.raft_node_id != recipient_node_id {
            // Ignore incorrectly routed message
            todo!()
        }
        if message.get_from() != sender_node_id {
            // Ignore malformed message
            todo!()
        }

        // Advance raft internal state by one step.
        self.mut_raft_node().step(message).unwrap();
    }

    fn make_raft_proposal(&mut self, proposal_contents: Vec<u8>) {
        self.mut_raft_node()
            .propose(vec![], proposal_contents)
            .unwrap();
    }

    fn make_raft_config_change_proposal(
        &mut self,
        node_id: u64,
        change_type: RaftConfigChangeType,
    ) {
        let config_change = create_raft_config_change(node_id, change_type);
        self.mut_raft_node()
            .propose_conf_change(vec![], config_change)
            .unwrap();
    }

    fn trigger_raft_tick(&mut self) {
        // Given that raft is being driven from the outside and arbitrary amount of time can
        // pass between driver invocation we may need to produce multiple ticks.
        while self.instant - self.tick_instant >= self.driver_config.tick_period {
            self.tick_instant += self.driver_config.tick_period;
            // invoke raft tick to trigger timer based changes.
            self.mut_raft_node().tick();
        }
    }

    fn apply_raft_committed_entries(
        &mut self,
        committed_entries: Vec<RaftEntry>,
    ) -> Result<(), PalError> {
        for committed_entry in committed_entries {
            if committed_entry.data.is_empty() {
                // Empty entry is produced by the newly elected leader to commit entries
                // from the previous terms.
                continue;
            }
            // The entry may either be a config change or a normal proposal.
            if let RaftEntryType::EntryConfChange = committed_entry.get_entry_type() {
                // Make committed configuration effective.
                let config_change = deserialize_config_change(&committed_entry.data).unwrap();
                let config_state = self
                    .mut_raft_node()
                    .apply_conf_change(&config_change)
                    .unwrap();

                self.collect_config_state(&config_state);

                self.mut_raft_node()
                    .store()
                    .wl()
                    .set_conf_state(config_state);
            } else {
                // Pass committed entry to the actor to make effective.
                self.actor
                    .on_apply_event(committed_entry.index, committed_entry.get_data())
                    .map_err(|_e| PalError::Internal)?;
            }
        }

        Ok(())
    }

    fn send_raft_messages(&mut self, raft_messages: Vec<RaftMessage>) -> Result<(), PalError> {
        for raft_message in raft_messages {
            // Buffer message to be sent out.
            self.send_message(EnvelopeOut {
                msg: Some(envelope_out::Msg::DeliverMessage(DeliverMessage {
                    recipient_node_id: raft_message.to,
                    sender_node_id: self.get_id(),
                    message_contents: serialize_raft_message(&raft_message).unwrap(),
                })),
            });
        }

        Ok(())
    }

    fn collect_leader_state(&mut self) {
        let raft_hard_state: HardState;
        let raft_soft_state: SoftState;
        {
            let raft_status = self.raft_node().status();
            raft_hard_state = raft_status.hs;
            raft_soft_state = raft_status.ss;
        }
        if raft_soft_state.raft_state == RaftStateRole::Leader {
            self.raft_state.leader_node_id = raft_soft_state.leader_id;
            self.raft_state.leader_term = raft_hard_state.term;
        }
    }

    fn collect_config_state(&mut self, config_state: &RaftConfState) {
        self.raft_state.committed_cluster_config = config_state.voters.clone();
    }

    fn restore_raft_snapshot(&mut self, raft_snapshot: &RaftSnapshot) -> Result<(), PalError> {
        if raft_snapshot.is_empty() {
            // Nothing to restore if the snapshot is empty.
            return Ok(());
        }

        self.collect_config_state(get_conf_state(raft_snapshot));

        // Persist unstable snapshot received from a peer into the stable storage.
        self.mut_raft_node()
            .store()
            .wl()
            .apply_snapshot(raft_snapshot.clone())
            .unwrap();

        // Pass snapshot to the actor to restore.
        self.actor
            .on_load_snapshot(raft_snapshot.get_data())
            .map_err(|_e| PalError::Internal)?;

        Ok(())
    }

    fn maybe_create_raft_snapshot(&mut self) -> Result<(), PalError> {
        let _actor_snapshot = self
            .actor
            .on_save_snapshot()
            .map_err(|_e| PalError::Internal)?;

        // todo!()

        Ok(())
    }

    fn advance_raft(&mut self) -> Result<(), PalError> {
        // Given that instant only set once trigger raft tick once as well.
        self.trigger_raft_tick();

        if !self.raft_node().has_ready() {
            // There is nothing process.
            return Ok(());
        }

        let mut raft_ready = self.mut_raft_node().ready();

        // Send out messages to the peers.
        self.send_raft_messages(raft_ready.take_messages())?;

        if let Some(raft_hard_state) = raft_ready.hs() {
            // Persist changed hard state into the stable storage.
            self.mut_raft_node()
                .store()
                .wl()
                .set_hardstate(raft_hard_state.clone());
        }

        // If not empty persist snapshot to stable storage and apply it to the
        // actor.
        self.restore_raft_snapshot(raft_ready.snapshot())?;

        // Apply committed entries to the actor state machine.
        self.apply_raft_committed_entries(raft_ready.take_committed_entries())?;
        // Send out messages that had to await the persistence of the hard state, entries
        // and snapshot to the stable storage.
        self.send_raft_messages(raft_ready.take_persisted_messages())?;

        if !raft_ready.entries().is_empty() {
            // Persist unstable entries into the stable storage.
            self.mut_raft_node()
                .store()
                .wl()
                .append(raft_ready.entries())
                .map_err(|_e| PalError::Internal)?;
        }

        // Advance raft state after fully processing ready.
        let mut light_raft_ready: raft::LightReady = self.mut_raft_node().advance(raft_ready);

        // Send out messages to the peers.
        self.send_raft_messages(light_raft_ready.take_messages())?;
        // Apply all committed entries.
        self.apply_raft_committed_entries(light_raft_ready.take_committed_entries())?;
        // Advance the apply index.
        self.mut_raft_node().advance_apply();

        Ok(())
    }

    fn preset_state_machine(&mut self, instant: u64) {
        self.instant = cmp::max(self.instant, instant);
        let instant = self.instant;
        let leader = self.check_raft_leadership();
        self.mut_core().set_state(instant, leader);
    }

    fn check_started(&self) -> Result<(), PalError> {
        let DriverState::Started = self.driver_state else {
            return Err(PalError::InvalidOperation);
        };

        Ok(())
    }

    fn process_init(&mut self, app_config: Vec<u8>, _app_signing_key: Vec<u8>, node_id_hint: u64) {
        let id = node_id_hint;
        self.mut_core().set_immutable_state(id, app_config);
        self.raft_node_id = id;
    }

    fn process_start_node(
        &mut self,
        start_node_request: &StartNodeRequest,
        actor_context: Box<dyn ActorContext>,
    ) -> Result<(), PalError> {
        let DriverState::Created = self.driver_state else {
            return Err(PalError::InvalidOperation);
        };

        self.initilize_raft_node(start_node_request.is_leader)?;

        self.actor
            .on_init(actor_context)
            .map_err(|_e| PalError::Internal)?;

        self.driver_state = DriverState::Started;

        self.send_message(EnvelopeOut {
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

        self.actor.on_shutdown();

        self.driver_state = DriverState::Stopped;

        Ok(())
    }

    fn process_change_cluster(
        &mut self,
        change_cluster_request: &ChangeClusterRequest,
    ) -> Result<(), PalError> {
        self.check_started()?;

        let change_type = match ChangeClusterType::from_i32(change_cluster_request.change_type) {
            Some(ChangeClusterType::ChangeTypeAddNode) => RaftConfigChangeType::AddNode,
            Some(ChangeClusterType::ChangeTypeRemoveNode) => RaftConfigChangeType::RemoveNode,
            _ => {
                self.log_entry(Severity::Error, "Unknown cluster change".to_string());

                return Ok(());
            }
        };

        self.make_raft_config_change_proposal(change_cluster_request.node_id, change_type);

        Ok(())
    }

    fn process_check_cluster(
        &mut self,
        _check_cluster_request: &CheckClusterRequest,
    ) -> Result<(), PalError> {
        self.check_started()?;

        self.collect_leader_state();

        self.send_message(EnvelopeOut {
            msg: Some(envelope_out::Msg::CheckCluster(CheckClusterResponse {
                leader_node_id: self.raft_state.leader_node_id,
                leader_term: self.raft_state.leader_term,
                cluster_node_ids: self.raft_state.committed_cluster_config.clone(),
                applied_change_id: 0,
                pending_change_id: 0,
            })),
        });

        Ok(())
    }

    fn process_deliver_message(
        &mut self,
        deliver_message: &DeliverMessage,
    ) -> Result<(), PalError> {
        self.check_started()?;

        self.make_raft_step(
            deliver_message.sender_node_id,
            deliver_message.recipient_node_id,
            &deliver_message.message_contents,
        );

        Ok(())
    }

    fn process_execute_proposal(
        &mut self,
        execute_proposal_request: &ExecuteProposalRequest,
    ) -> Result<(), PalError> {
        self.check_started()?;

        self.actor
            .on_process_command(execute_proposal_request.proposal_contents.as_ref())
            .map_err(|_e| PalError::Internal)?;

        Ok(())
    }

    fn process_actor_output(&mut self) {
        let (proposals, messages) = self.mut_core().take_outputs();

        for proposal in proposals {
            self.make_raft_proposal(proposal);
        }

        for message in messages {
            self.send_message(message);
        }
    }

    fn process_state_machine(&mut self) -> Result<Vec<EnvelopeOut>, PalError> {
        // Advance raft internal state.
        self.advance_raft()?;

        // Maybe create a snashot of the actor to reduce the size of the log.
        self.maybe_create_raft_snapshot()?;

        // Take messages to be sent out.
        Ok(mem::take(&mut self.messages))
    }

    fn get_id(&self) -> u64 {
        self.raft_node_id
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

impl Application for Driver {
    fn receive_messages(
        &mut self,
        host: &mut impl Host,
        instant: u64,
        in_messages: &[MessageEnvelope],
    ) -> Result<(), PalError> {
        self.preset_state_machine(instant);

        for serialized_message in in_messages {
            let deserialized_message =
                EnvelopeIn::decode(serialized_message.as_ref()).map_err(|_e| PalError::Decoding)?;
            let message = deserialized_message.msg.ok_or(PalError::UnknownMessage)?;

            match message {
                envelope_in::Msg::StartNode(ref start_node_request) => {
                    let actor_context = Box::new(DriverContext::new(Rc::clone(&self.core)));

                    self.process_init(
                        host.get_self_config(),
                        host.get_self_attestation().public_signing_key(),
                        start_node_request.node_id_hint,
                    );

                    self.process_start_node(start_node_request, actor_context)
                }
                envelope_in::Msg::StopNode(ref stop_node_request) => {
                    self.process_stop_node(stop_node_request)
                }
                envelope_in::Msg::ChangeCluster(ref change_cluster_request) => {
                    self.process_change_cluster(change_cluster_request)
                }
                envelope_in::Msg::CheckCluster(ref check_cluster_request) => {
                    self.process_check_cluster(check_cluster_request)
                }
                envelope_in::Msg::DeliverMessage(ref deliver_message) => {
                    self.process_deliver_message(deliver_message)
                }
                envelope_in::Msg::ExecuteProposal(ref execute_proposal_request) => {
                    self.process_execute_proposal(execute_proposal_request)
                }
            }?;
        }

        self.process_actor_output();

        let out_messages = self.process_state_machine()?;
        self.send_messages(host, out_messages);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}
