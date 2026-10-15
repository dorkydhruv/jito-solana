//! Core scheduler engine driving Agave block production and BAM batch execution

pub use crate::bam::client::OutboundMsg;

use {
    crate::{
        config::Config,
        scheduler::{
            progress::ProgressTracker,
            shmem::{Free, Slice, allocate, allocate_batch},
        },
    },
    agave_scheduler_bindings::{
        ExecutionWorkerToPackMessage, LEADER_READY, LEADER_STARTING, PackToExecutionWorkerMessage,
        SharableTransactionBatchRegion, SharableTransactionRegion, execution_message_flags,
        processed_codes,
        worker_message_types::{ExecutionResponse, not_included_reasons},
    },
    agave_scheduler_handshake::ClientWorkerSession,
    jito_protos::proto::bam_types::{
        self, AtomicTxnBatchResult, Committed, LeaderState, NotCommitted,
        TransactionCommittedResult, TransactionError, TransactionErrorReason,
        atomic_txn_batch_result::Result as BatchResultEnum,
        not_committed::Reason as NotCommittedReason,
    },
    log::{info, warn},
    rdtsc::Instant,
    rts_alloc::Allocator,
    rustc_hash::FxHashMap,
    solana_keypair::Keypair,
    solana_signer::Signer,
    std::sync::Arc,
    tip_manager::{BlockBuilderFeeInfo, TipAccountData, TipManager},
    tokio::sync::{mpsc, oneshot, watch},
};

/// Worker admission bound: limit in-flight batches to NUM_WORKERS (PR #1675)
pub const MAX_INFLIGHT_BATCHES: usize = 8;
/// Maximum number of atomic batches ingested per burst (PR #1712)
pub const ATOMIC_TXN_BATCH_BURST: usize = 32;

struct InflightBatch {
    seq_id: u32,
    #[allow(dead_code)]
    slot: u64,
    batch: SharableTransactionBatchRegion,
    #[allow(dead_code)]
    num_txs: usize,
    #[allow(dead_code)]
    dispatched_at: Instant,
}

pub struct SchedulerEngine {
    allocator: Allocator,
    progress: ProgressTracker,
    pack_to_worker: Vec<shaq::spsc::Producer<PackToExecutionWorkerMessage>>,
    worker_to_pack: Vec<shaq::spsc::Consumer<ExecutionWorkerToPackMessage>>,
    num_workers: usize,
    next_worker: usize,
    batch_rx: crossbeam_channel::Receiver<bam_types::MultipleAtomicTxnBatch>,
    outbound_tx: mpsc::Sender<OutboundMsg>,
    identity_rx: watch::Receiver<Option<Arc<Keypair>>>,
    tip_manager_rx: watch::Receiver<Option<Arc<TipManager>>>,
    config: Config,
    current_slot: u64,
    current_progress: u8,
    is_leader: bool,
    inflight: FxHashMap<usize, InflightBatch>, // keyed by transactions_offset
}

impl SchedulerEngine {
    pub fn new(
        allocator: Allocator,
        progress: ProgressTracker,
        workers: Vec<ClientWorkerSession>,
        batch_rx: crossbeam_channel::Receiver<bam_types::MultipleAtomicTxnBatch>,
        outbound_tx: mpsc::Sender<OutboundMsg>,
        identity_rx: watch::Receiver<Option<Arc<Keypair>>>,
        tip_manager_rx: watch::Receiver<Option<Arc<TipManager>>>,
        config: Config,
    ) -> Self {
        let num_workers = workers.len();
        let (pack_to_worker, worker_to_pack): (Vec<_>, Vec<_>) = workers
            .into_iter()
            .map(|s| (s.pack_to_worker, s.worker_to_pack))
            .unzip();

        Self {
            allocator,
            progress,
            pack_to_worker,
            worker_to_pack,
            num_workers,
            next_worker: 0,
            batch_rx,
            outbound_tx,
            identity_rx,
            tip_manager_rx,
            config,
            current_slot: 0,
            current_progress: 0,
            is_leader: false,
            inflight: FxHashMap::default(),
        }
    }

    /// Run the main scheduling loop
    pub fn run(&mut self, mut shutdown_rx: oneshot::Receiver<()>) {
        info!("Scheduler engine loop running with {} workers", self.num_workers);

        while shutdown_rx.try_recv().is_err() {
            // 1. Poll validator progress feed
            let poll_result = self.progress.poll().map(|m| *m);
            match poll_result {
                Ok(msg) => {
                    let new_leader = msg.leader_state == LEADER_STARTING || msg.leader_state == LEADER_READY;
                    let slot_changed = msg.current_slot != self.current_slot;
                    let progress_changed = msg.current_slot_progress != self.current_progress;

                    if slot_changed || (new_leader != self.is_leader) {
                        self.handle_slot_transition(msg.current_slot, new_leader);
                    }

                    if self.is_leader && (slot_changed || progress_changed) {
                        let cu_remaining = if msg.remaining_cost_units <= u32::MAX as u64 {
                            msg.remaining_cost_units as u32
                        } else {
                            u32::MAX
                        };
                        self.broadcast_leader_state(
                            msg.current_slot,
                            msg.current_slot_progress as u32,
                            cu_remaining,
                        );
                    }

                    self.current_slot = msg.current_slot;
                    self.current_progress = msg.current_slot_progress;
                    self.is_leader = new_leader;
                }
                Err(e) => {
                    warn!("Progress tracker poll error: {e:#}");
                }
            }

            // 2. Resolve completed worker responses
            self.resolve_workers();

            // 3. If leader, ingest incoming BAM batches up to capacity guard
            if self.is_leader {
                self.ingest_bam_batches();
            } else {
                // Drain and discard any stale batches if not leader
                while let Ok(_stale) = self.batch_rx.try_recv() {}
            }

            // Clean remote free lists periodically
            self.allocator.clean_remote_frees();

            // Brief yield to avoid burning 100% CPU when queues are quiet
            std::thread::yield_now();
        }

        self.drain_all_inflight();
        info!("Scheduler engine loop exited");
    }

    fn handle_slot_transition(&mut self, new_slot: u64, is_leader: bool) {
        if is_leader {
            info!("Entered leader slot {new_slot}");
            self.crank_tip_programs(new_slot);
        } else if self.is_leader {
            info!("Exited leader window; cleaning in-flight batches");
            self.drain_all_inflight();
        }
    }

    fn broadcast_leader_state(&self, slot: u64, tick: u32, slot_cu_budget_remaining: u32) {
        let state = LeaderState {
            slot,
            tick,
            slot_cu_budget_remaining,
        };
        let _ = self.outbound_tx.try_send(OutboundMsg::LeaderState(state));
    }

    fn crank_tip_programs(&mut self, slot: u64) {
        let Some(tip_manager) = self.tip_manager_rx.borrow().clone() else {
            return;
        };
        let Some(identity) = self.identity_rx.borrow().clone() else {
            return;
        };

        let blockhash = self.progress.blockhash();
        let fee_info = BlockBuilderFeeInfo {
            block_builder: self.config.block_builder.unwrap_or_else(|| identity.pubkey()),
            block_builder_commission: self.config.block_builder_commission,
        };

        let account_data = TipAccountData {
            epoch: slot / 432_000,
            ..Default::default()
        };

        if let Ok(bundle) = tip_manager.get_initialize_tip_programs_bundle(
            &account_data,
            blockhash,
            &identity,
        ) {
            for tx_bytes in bundle {
                self.dispatch_internal_transaction(&tx_bytes, slot);
            }
        }

        let memo_ix = spl_memo_interface::instruction::build_memo(
            &spl_memo_interface::v3::id(),
            b"JTO",
            &[],
        );
        if let Ok(bundle) = tip_manager.get_tip_programs_crank_bundle(
            &account_data,
            &identity,
            &fee_info,
            blockhash,
            memo_ix,
        ) {
            for tx_bytes in bundle {
                self.dispatch_internal_transaction(&tx_bytes, slot);
            }
        }
    }

    fn dispatch_internal_transaction(&mut self, tx_bytes: &[u8], slot: u64) {
        let region = allocate(tx_bytes, &self.allocator);
        let batch = allocate_batch([region], &self.allocator);
        let worker = 0;
        let msg = PackToExecutionWorkerMessage {
            flags: 0,
            max_working_slot: slot,
            batch,
        };
        let _ = self.pack_to_worker[worker].try_write(msg);
    }

    fn ingest_bam_batches(&mut self) {
        let mut ingested_burst = 0;

        while self.inflight.len() < MAX_INFLIGHT_BATCHES && ingested_burst < ATOMIC_TXN_BATCH_BURST {
            let Ok(multiple_batch) = self.batch_rx.try_recv() else {
                break;
            };

            for atomic_batch in multiple_batch.batches {
                if self.inflight.len() >= MAX_INFLIGHT_BATCHES {
                    break;
                }
                if atomic_batch.packets.is_empty() {
                    continue;
                }

                self.dispatch_atomic_batch(atomic_batch);
                ingested_burst += 1;
            }
        }
    }

    fn dispatch_atomic_batch(&mut self, batch: bam_types::AtomicTxnBatch) {
        let seq_id = batch.seq_id;
        let max_slot = batch.max_schedule_slot;
        let num_txs = batch.packets.len();

        let regions: Vec<SharableTransactionRegion> = batch
            .packets
            .into_iter()
            .map(|pkt| allocate(&pkt.data, &self.allocator))
            .collect();

        let batch_region = allocate_batch(regions, &self.allocator);
        let offset = batch_region.transactions_offset;

        let worker = self.next_worker;
        self.next_worker = (self.next_worker + 1) % self.num_workers;

        let flags = execution_message_flags::ALL_OR_NOTHING
            | execution_message_flags::DROP_ON_FAILURE;

        let message = PackToExecutionWorkerMessage {
            flags,
            max_working_slot: max_slot,
            batch: batch_region,
        };

        if self.pack_to_worker[worker].try_write(message).is_err() {
            // Worker queue full; free batch memory
            batch_region.free_full(&self.allocator);
            return;
        }

        self.inflight.insert(
            offset,
            InflightBatch {
                seq_id,
                slot: max_slot,
                batch: batch_region,
                num_txs,
                dispatched_at: Instant::now(),
            },
        );
    }

    fn resolve_workers(&mut self) {
        for worker in 0..self.num_workers {
            while let Some(msg) = self.worker_to_pack[worker].try_read() {
                let offset = msg.batch.transactions_offset;
                let Some(inflight) = self.inflight.remove(&offset) else {
                    // Internal transaction or unknown batch
                    msg.free_full(&self.allocator);
                    continue;
                };

                let result = if msg.processed_code == processed_codes::PROCESSED {
                    self.build_batch_result(inflight.seq_id, &msg)
                } else {
                    // Timeout or dropped
                    AtomicTxnBatchResult {
                        seq_id: inflight.seq_id,
                        result: Some(BatchResultEnum::NotCommitted(NotCommitted {
                            reason: Some(NotCommittedReason::SchedulingError(
                                bam_types::SchedulingError::PohTimeout as i32,
                            )),
                        })),
                    }
                };

                let _ = self.outbound_tx.try_send(OutboundMsg::BatchResult(result));
                msg.free_full(&self.allocator);
            }
        }
    }

    fn build_batch_result(
        &self,
        seq_id: u32,
        msg: &ExecutionWorkerToPackMessage,
    ) -> AtomicTxnBatchResult {
        let responses: &[ExecutionResponse] = msg.responses.slice(&self.allocator);
        Self::build_batch_result_from_responses(seq_id, responses)
    }

    pub fn map_not_included_reason(reason_code: u8) -> TransactionErrorReason {
        match reason_code {
            not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE
            | not_included_reasons::PARTIAL_BATCH_CANCELLED
            | not_included_reasons::BANK_NOT_AVAILABLE => TransactionErrorReason::CommitCancelled,
            code if code >= 64 => {
                TransactionErrorReason::try_from((code - 64) as i32)
                    .unwrap_or(TransactionErrorReason::InstructionError)
            }
            _ => TransactionErrorReason::InstructionError,
        }
    }

    pub fn build_batch_result_from_responses(
        seq_id: u32,
        responses: &[ExecutionResponse],
    ) -> AtomicTxnBatchResult {
        // Check if all transactions committed successfully
        let failed_idx = responses
            .iter()
            .position(|r| r.not_included_reason != not_included_reasons::NONE);

        match failed_idx {
            None => {
                let tx_results: Vec<TransactionCommittedResult> = responses
                    .iter()
                    .map(|r| TransactionCommittedResult {
                        cus_consumed: r.cost_units as u32,
                        feepayer_balance_lamports: r.fee_payer_balance,
                        loaded_accounts_data_size: 0,
                        execution_success: true,
                    })
                    .collect();

                AtomicTxnBatchResult {
                    seq_id,
                    result: Some(BatchResultEnum::Committed(Committed {
                        transaction_results: tx_results,
                    })),
                }
            }
            Some(idx) => {
                let proto_reason = Self::map_not_included_reason(responses[idx].not_included_reason);

                AtomicTxnBatchResult {
                    seq_id,
                    result: Some(BatchResultEnum::NotCommitted(NotCommitted {
                        reason: Some(NotCommittedReason::TransactionError(TransactionError {
                            index: idx as u32,
                            reason: proto_reason as i32,
                        })),
                    })),
                }
            }
        }
    }

    fn drain_all_inflight(&mut self) {
        for (_offset, inflight) in self.inflight.drain() {
            let result = AtomicTxnBatchResult {
                seq_id: inflight.seq_id,
                result: Some(BatchResultEnum::NotCommitted(NotCommitted {
                    reason: Some(NotCommittedReason::SchedulingError(
                        bam_types::SchedulingError::OutsideLeaderSlot as i32,
                    )),
                })),
            };
            let _ = self.outbound_tx.try_send(OutboundMsg::BatchResult(result));
            inflight.batch.free_full(&self.allocator);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_batch_result_all_committed() {
        let responses = vec![
            ExecutionResponse {
                execution_slot: 100,
                not_included_reason: not_included_reasons::NONE,
                cost_units: 5000,
                fee_payer_balance: 1_000_000,
            },
            ExecutionResponse {
                execution_slot: 100,
                not_included_reason: not_included_reasons::NONE,
                cost_units: 12000,
                fee_payer_balance: 900_000,
            },
        ];

        let result = SchedulerEngine::build_batch_result_from_responses(42, &responses);
        assert_eq!(result.seq_id, 42);

        match result.result {
            Some(BatchResultEnum::Committed(committed)) => {
                assert_eq!(committed.transaction_results.len(), 2);
                assert_eq!(committed.transaction_results[0].cus_consumed, 5000);
                assert_eq!(committed.transaction_results[0].feepayer_balance_lamports, 1_000_000);
                assert!(committed.transaction_results[0].execution_success);
                assert_eq!(committed.transaction_results[1].cus_consumed, 12000);
                assert_eq!(committed.transaction_results[1].feepayer_balance_lamports, 900_000);
                assert!(committed.transaction_results[1].execution_success);
            }
            _ => panic!("Expected BatchResultEnum::Committed"),
        }
    }

    #[test]
    fn test_build_batch_result_not_committed_account_in_use() {
        let responses = vec![
            ExecutionResponse {
                execution_slot: 100,
                not_included_reason: not_included_reasons::NONE,
                cost_units: 5000,
                fee_payer_balance: 1_000_000,
            },
            ExecutionResponse {
                execution_slot: 100,
                not_included_reason: not_included_reasons::ACCOUNT_IN_USE,
                cost_units: 0,
                fee_payer_balance: 1_000_000,
            },
        ];

        let result = SchedulerEngine::build_batch_result_from_responses(43, &responses);
        assert_eq!(result.seq_id, 43);

        match result.result {
            Some(BatchResultEnum::NotCommitted(not_committed)) => match not_committed.reason {
                Some(NotCommittedReason::TransactionError(tx_err)) => {
                    assert_eq!(tx_err.index, 1);
                    assert_eq!(tx_err.reason, TransactionErrorReason::AccountInUse as i32);
                }
                _ => panic!("Expected TransactionError"),
            },
            _ => panic!("Expected BatchResultEnum::NotCommitted"),
        }
    }

    #[test]
    fn test_build_batch_result_not_committed_blockhash_not_found() {
        let responses = vec![ExecutionResponse {
            execution_slot: 100,
            not_included_reason: not_included_reasons::BLOCKHASH_NOT_FOUND,
            cost_units: 0,
            fee_payer_balance: 0,
        }];

        let result = SchedulerEngine::build_batch_result_from_responses(44, &responses);
        match result.result {
            Some(BatchResultEnum::NotCommitted(not_committed)) => match not_committed.reason {
                Some(NotCommittedReason::TransactionError(tx_err)) => {
                    assert_eq!(tx_err.index, 0);
                    assert_eq!(tx_err.reason, TransactionErrorReason::BlockhashNotFound as i32);
                }
                _ => panic!("Expected TransactionError"),
            },
            _ => panic!("Expected BatchResultEnum::NotCommitted"),
        }
    }

    #[test]
    fn test_build_batch_result_all_or_nothing_failure() {
        let responses = vec![ExecutionResponse {
            execution_slot: 100,
            not_included_reason: not_included_reasons::ALL_OR_NOTHING_BATCH_FAILURE,
            cost_units: 0,
            fee_payer_balance: 0,
        }];

        let result = SchedulerEngine::build_batch_result_from_responses(45, &responses);
        match result.result {
            Some(BatchResultEnum::NotCommitted(not_committed)) => match not_committed.reason {
                Some(NotCommittedReason::TransactionError(tx_err)) => {
                    assert_eq!(tx_err.index, 0);
                    assert_eq!(tx_err.reason, TransactionErrorReason::CommitCancelled as i32);
                }
                _ => panic!("Expected TransactionError"),
            },
            _ => panic!("Expected BatchResultEnum::NotCommitted"),
        }
    }
}
