extern crate alloc;
extern crate core;
extern crate hashbrown;
extern crate prost;
extern crate trusted;

pub mod counter {
    #![allow(non_snake_case)]
    include!(concat!(env!("OUT_DIR"), "/counter.rs"));
}

use crate::counter::{
    counter_request, counter_response, CounterCompareAndSwapRequest, CounterCompareAndSwapResponse,
    CounterConfig, CounterRequest, CounterResponse, CounterSnapshot, CounterStatus,
};
use alloc::collections::BTreeMap;
use alloc::string::String;
use core::cell::RefCell;
use hashbrown::HashMap;
use prost::Message;
use trusted::{
    driver::{Driver, DriverConfig},
    endpoint::*,
    model::{Actor, ActorContext, ActorError},
    platform::{Application, Attestation, Host, MessageEnvelope, PalError},
};

struct CounterActor {
    context: Option<Box<dyn ActorContext>>,
    values: HashMap<String, i64>,
}

impl CounterActor {
    pub fn new() -> Self {
        CounterActor {
            context: None,
            values: HashMap::new(),
        }
    }

    fn get_context(&mut self) -> &mut dyn ActorContext {
        self.context.as_mut().expect("context").as_mut()
    }

    fn send_message<M: Message>(&mut self, message: &M) {
        self.get_context().send_message(message.encode_to_vec())
    }

    fn apply_compare_and_swap(
        &mut self,
        id: u64,
        counter_name: &String,
        compare_and_swap_request: &CounterCompareAndSwapRequest,
    ) -> CounterResponse {
        let mut response = CounterResponse {
            id,
            status: CounterStatus::Unspecified.into(),
            op: None,
        };
        let mut compare_and_swap_response = CounterCompareAndSwapResponse {
            ..Default::default()
        };

        let existing_value_ref = self.values.entry_ref(counter_name);
        let existing_value = existing_value_ref.or_insert(0);
        compare_and_swap_response.old_value = *existing_value;
        if *existing_value == compare_and_swap_request.expected_value {
            *existing_value = compare_and_swap_request.new_value;

            response.status = CounterStatus::Success.into();
            compare_and_swap_response.new_value = compare_and_swap_request.new_value;
        } else {
            response.status = CounterStatus::InvalidArgumentError.into();
        }
        response.op = Some(counter_response::Op::CompareAndSwap(
            compare_and_swap_response,
        ));

        response
    }
}

impl Actor for CounterActor {
    fn on_init(&mut self, context: Box<dyn ActorContext>) -> Result<(), ActorError> {
        self.context = Some(context);
        self.values = HashMap::new();
        let config = CounterConfig::decode(self.get_context().get_config().as_ref())
            .map_err(|_e| ActorError::Decoding)?;
        for (counter_name, counter_value) in config.initial_values {
            self.values.insert(counter_name, counter_value);
        }

        Ok(())
    }

    fn on_shutdown(&mut self) {}

    fn on_save_snapshot(&mut self) -> Result<Vec<u8>, ActorError> {
        let mut snapshot = CounterSnapshot {
            values: BTreeMap::new(),
        };
        for (counter_name, counter_value) in &self.values {
            snapshot
                .values
                .insert(counter_name.to_string(), *counter_value);
        }

        Ok(snapshot.encode_to_vec())
    }

    fn on_load_snapshot(&mut self, snapshot: &[u8]) -> Result<(), ActorError> {
        let snapshot = CounterSnapshot::decode(snapshot).map_err(|_e| ActorError::Decoding)?;
        for (counter_name, counter_value) in snapshot.values {
            self.values.insert(counter_name, counter_value);
        }

        Ok(())
    }

    fn on_process_command(&mut self, command: &[u8]) -> Result<(), ActorError> {
        let request = CounterRequest::decode(command).map_err(|_e| ActorError::Decoding)?;
        let mut response = CounterResponse {
            id: request.id,
            ..Default::default()
        };
        let mut status = CounterStatus::Success;
        if request.op.is_none() {
            status = CounterStatus::InvalidArgumentError;
        }
        if !self.get_context().is_leader() {
            status = CounterStatus::NotLeaderError;
        }

        if let CounterStatus::Success = status {
            self.get_context().propose_event(command.to_vec())?;
        } else {
            response.status = status.into();
            self.send_message(&response);
        }

        Ok(())
    }

    fn on_apply_event(&mut self, _index: u64, event: &[u8]) -> Result<(), ActorError> {
        let request = CounterRequest::decode(event).map_err(|_e| ActorError::Decoding)?;
        let op = request.op.unwrap();

        let response = match op {
            counter_request::Op::CompareAndSwap(ref compare_and_swap_request) => {
                self.apply_compare_and_swap(request.id, &request.name, compare_and_swap_request)
            }
        };
        self.send_message(&response);

        Ok(())
    }
}

struct FakeCluster {
    platforms: Vec<FakePlatform>,
    leader_id: usize,
}

impl FakeCluster {
    fn new(size: usize) -> FakeCluster {
        let mut cluster = FakeCluster {
            platforms: Vec::with_capacity(size),
            leader_id: 1,
        };
        for i in 1..size + 1 {
            cluster.platforms.push(FakePlatform::new(i as u64));
        }

        cluster
    }

    fn start(&mut self) {
        for i in 0..self.platforms.len() {
            self.platforms
                .get_mut(i)
                .unwrap()
                .send_start_node(i + 1 == self.leader_id as usize);
        }

        self.advance(0);
    }

    fn add_node_to_cluster(&mut self, node_id: u64) {
        let leader_platform: &mut FakePlatform = self.platforms.get_mut(self.leader_id).unwrap();
        leader_platform.send_change_cluster(0, node_id, ChangeClusterType::ChangeTypeAddNode);
    }

    fn add_nodes_to_cluster(&mut self) {
        for node_id in 1..self.platforms.len() + 1 {
            if node_id == self.leader_id {
                continue;
            }
            self.add_node_to_cluster(node_id as u64);
        }
    }

    fn platform(&mut self, id: usize) -> &mut FakePlatform {
        self.platforms.get_mut(id).unwrap()
    }

    fn advance(&mut self, duration: u64) {
        for platform in &mut self.platforms {
            platform.advance_time(duration);
            platform.send_messages_in();
        }
    }

    fn stop(&mut self) {}
}

struct FakePlatform {
    id: u64,
    messages_in: Vec<EnvelopeIn>,
    instant: u64,
    driver: RefCell<Driver>,
    host: RefCell<FakeHost>,
}

impl FakePlatform {
    fn new(id: u64) -> FakePlatform {
        FakePlatform {
            id,
            messages_in: Vec::new(),
            instant: 0,
            driver: RefCell::new(Driver::new(
                DriverConfig {
                    tick_period: 100,
                    snapshot_count: 1000,
                },
                Box::new(CounterActor::new()),
            )),
            host: RefCell::new(FakeHost::new()),
        }
    }

    fn send_start_node(&mut self, is_leader: bool) {
        self.append_meessage_in(EnvelopeIn {
            msg: Some(envelope_in::Msg::StartNode(StartNodeRequest {
                is_leader,
                node_id_hint: self.id,
            })),
        });
    }

    fn send_stop_node(&mut self) {
        self.append_meessage_in(EnvelopeIn {
            msg: Some(envelope_in::Msg::StopNode(StopNodeRequest {})),
        });
    }

    fn send_change_cluster(
        &mut self,
        change_id: u64,
        node_id: u64,
        change_type: ChangeClusterType,
    ) {
        self.append_meessage_in(EnvelopeIn {
            msg: Some(envelope_in::Msg::ChangeCluster(ChangeClusterRequest {
                change_id,
                node_id,
                change_type: change_type.into(),
            })),
        });
    }

    fn advance_time(&mut self, duration: u64) {
        self.instant += duration;
    }

    fn append_meessage_in(&mut self, message_in: EnvelopeIn) {
        self.messages_in.push(message_in)
    }

    fn send_messages_in(&mut self) {
        let mut messages: Vec<MessageEnvelope> = Vec::with_capacity(self.messages_in.len());
        for message_in in &self.messages_in {
            messages.push(message_in.encode_to_vec());
        }
        self.messages_in.clear();

        let mut driver = self.driver.borrow_mut();
        let mut host = self.host.borrow_mut();

        driver
            .receive_messages(&mut *host, self.instant, &messages)
            .unwrap()
    }
}

struct FakeHost {
    config: Vec<u8>,
    messages_out: Vec<EnvelopeOut>,
}

impl FakeHost {
    fn new() -> FakeHost {
        FakeHost {
            config: CounterConfig {
                initial_values: BTreeMap::new(),
            }
            .encode_to_vec(),
            messages_out: Vec::new(),
        }
    }

    fn take_messages_out(&mut self) -> Vec<EnvelopeOut> {
        core::mem::take(&mut self.messages_out)
    }
}

impl Host for FakeHost {
    fn get_self_attestation(&self) -> Box<dyn Attestation> {
        Box::new(FakeAttestation {})
    }

    fn get_self_config(&self) -> Vec<u8> {
        self.config.clone()
    }

    fn send_messages(&mut self, messages: &[MessageEnvelope]) -> Result<(), PalError> {
        for message_envelope in messages {
            self.messages_out
                .push(EnvelopeOut::decode(message_envelope.as_ref()).unwrap());
        }

        Ok(())
    }

    fn verify_peer_attestation(
        &self,
        _peer_attestation: &[u8],
    ) -> Result<Box<dyn Attestation>, PalError> {
        todo!()
    }
}

struct FakeAttestation {}

impl Attestation for FakeAttestation {
    fn serialize(&self) -> Result<Vec<u8>, PalError> {
        todo!()
    }

    fn sign(&self, _data: &[u8]) -> Result<Vec<u8>, PalError> {
        todo!()
    }

    fn verify(&self, _data: &[u8], _signature: &[u8]) -> Result<(), PalError> {
        todo!()
    }

    fn public_signing_key(&self) -> Vec<u8> {
        Vec::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn integration() {
        let cluster_size = 3;

        let mut cluster = FakeCluster::new(cluster_size);

        cluster.start();

        cluster.stop();
    }
}
