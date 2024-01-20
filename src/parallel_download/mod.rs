mod download;
mod local_storage;
mod peer_aggregator;
mod protocol;

pub mod util;

pub use download::download;
pub use local_storage::{Command as LSCommand, LocalStorage};
pub use protocol::block_status_handler;

pub fn get_block_size(_size: usize) -> usize {
    8 * 1024 * 1024 // 8 MiB
}
