// extern crate anyhow;
extern crate alloc;
extern crate core;
extern crate hashbrown;
extern crate prost;
extern crate trusted_model;

pub mod counter {
    include!(concat!(env!("OUT_DIR"), "/counter.rs"));
}

use crate::counter::{
    counter_request, counter_response, CounterCompareAndSwapRequest, CounterCompareAndSwapResponse,
    CounterConfig, CounterRequest, CounterResponse, CounterSnapshot, CounterStatus,
};
use alloc::collections::BTreeMap;
use alloc::string::String;
use hashbrown::HashMap;
use prost::Message;
use trusted_model::model::{Actor, ActorContext, ActorError};

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
        self.get_context()
            .send_message(message.encode_to_vec().as_ref())
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
            self.get_context().propose_event(command)?;
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

fn main() {
    let _acounter_actor = CounterActor::new();
}
