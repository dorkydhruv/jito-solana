//! Agave validator IPC progress tracker

use {
    agave_scheduler_bindings::ProgressMessage,
    anyhow::{Result, bail},
    rdtsc::Instant,
    solana_hash::Hash,
};

const PROGRESS_MESSAGE_TIMEOUT_MS: u64 = 400;

pub struct ProgressTracker {
    consumer: shaq::spsc::Consumer<ProgressMessage>,
    last: ProgressMessage,
    last_received: Instant,
}

impl ProgressTracker {
    pub fn new(consumer: shaq::spsc::Consumer<ProgressMessage>) -> Self {
        Self {
            consumer,
            // SAFETY: ProgressMessage is repr(C) with only primitive fields; all-zero is valid bit pattern
            last: unsafe { std::mem::zeroed() },
            last_received: Instant::now(),
        }
    }

    /// Poll for latest progress message from validator
    pub fn poll(&mut self) -> Result<&ProgressMessage> {
        let mut got_msg = false;
        while let Some(msg) = self.consumer.try_read() {
            self.last = msg;
            got_msg = true;
        }
        if got_msg {
            self.last_received = Instant::now();
        } else if self.last_received.elapsed_ms() > PROGRESS_MESSAGE_TIMEOUT_MS {
            bail!("IPC progress feed stale (> {PROGRESS_MESSAGE_TIMEOUT_MS}ms)");
        }
        Ok(&self.last)
    }

    #[allow(dead_code)]
    pub fn last(&self) -> &ProgressMessage {
        &self.last
    }

    #[allow(dead_code)]
    pub fn blockhash(&self) -> Hash {
        Hash::new_from_array(self.last.latest_blockhash)
    }
}
