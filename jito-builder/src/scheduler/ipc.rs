//! IPC handshake connection with Agave validator

use {
    agave_scheduler_handshake::{
        ClientHandshakeError, ClientLogon, ClientSession, client::connect as handshake_connect,
    },
    log::{debug, info},
    std::{path::Path, time::Duration},
};

pub const ALLOCATOR_SIZE: usize = 64 * 1024 * 1024; // 64 MB
pub const NUM_ALLOCATOR_HANDLES: usize = 16;
pub const TPU_TO_PACK_CAPACITY: usize = 4096;
pub const PROGRESS_TRACKER_CAPACITY: usize = 128;
pub const PACK_TO_WORKER_CAPACITY: usize = 128;
pub const WORKER_TO_PACK_CAPACITY: usize = 128;
pub const CHECK_WORKER_CAPACITY: usize = 128;
const IPC_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

pub fn connect(
    socket_path: &Path,
    num_workers: usize,
) -> Result<ClientSession, ClientHandshakeError> {
    debug!("Connecting to Agave validator scheduler IPC at {}", socket_path.display());
    let session = handshake_connect(
        socket_path,
        ClientLogon {
            worker_count: num_workers,
            check_worker_count: 1,
            allocator_size: ALLOCATOR_SIZE,
            allocator_handles: NUM_ALLOCATOR_HANDLES,
            tpu_to_pack_capacity: TPU_TO_PACK_CAPACITY,
            progress_tracker_capacity: PROGRESS_TRACKER_CAPACITY,
            pack_to_worker_capacity: PACK_TO_WORKER_CAPACITY,
            worker_to_pack_capacity: WORKER_TO_PACK_CAPACITY,
            pack_to_check_worker_capacity: CHECK_WORKER_CAPACITY,
            check_worker_to_pack_capacity: CHECK_WORKER_CAPACITY,
            flags: 0,
        },
        IPC_HANDSHAKE_TIMEOUT,
    )?;
    info!("Connected to Agave validator scheduler IPC: workers={num_workers}");
    Ok(session)
}
