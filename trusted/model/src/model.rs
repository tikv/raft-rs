use alloc::boxed::Box;
use alloc::vec::Vec;
use core::error::Error;
use core::fmt;
use core::option::Option;
use core::result::Result;

#[derive(Debug)]
pub enum ActorError {
    Decoding,
    Internal,
}

impl Error for ActorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ActorError::Decoding => write!(f, "Failed to decode"),
            ActorError::Internal => write!(f, "Intern error"),
        }
    }
}

pub trait ActorContext {
    fn get_instant(&self) -> u64;

    fn get_config(&self) -> Vec<u8>;

    fn is_leader(&self) -> bool;

    fn propose_event(&mut self, event: &[u8]) -> Result<(), ActorError>;

    fn send_message(&mut self, message: &[u8]) -> ();

    fn log_entry(&mut self, entry: &[u8]) -> ();
}

pub trait Actor {
    fn on_init(&mut self, context: Box<dyn ActorContext>) -> Result<(), ActorError>;

    fn on_shutdown(&mut self) -> ();

    fn on_save_snapshot(&mut self) -> Result<Vec<u8>, ActorError>;

    fn on_load_snapshot(&mut self, snapshot: &[u8]) -> Result<(), ActorError>;

    fn on_process_command(&mut self, command: &[u8]) -> Result<(), ActorError>;

    fn on_apply_event(&mut self, index: u64, event: &[u8]) -> Result<(), ActorError>;
}
