use {
    agave_scheduler_bindings::{
        ExecutionResponseRegion, ExecutionWorkerToPackMessage, LEADER_READY,
        PackToExecutionWorkerMessage, ProgressMessage, execution_message_flags,
        processed_codes, worker_message_types::{ExecutionResponse, not_included_reasons},
    },
    agave_scheduler_handshake::server::Server,
    bytes::Bytes,
    crossbeam_channel::unbounded,
    jito_builder::{
        bam::client::{BamClient, OutboundMsg},
        config::Config,
        scheduler::{
            engine::SchedulerEngine,
            ipc::connect as ipc_connect,
            progress::ProgressTracker,
        },
    },
    jito_protos::proto::{
        bam_api::{
            AuthChallengeRequest, AuthChallengeResponse, ConfigRequest, ConfigResponse,
            SchedulerMessage, SchedulerMessageV0, SchedulerResponse, SchedulerResponseV0,
            bam_node_api_server::{BamNodeApi, BamNodeApiServer},
            scheduler_message,
            scheduler_message_v0::Msg,
            scheduler_response_v0::Resp,
        },
        bam_types::{
            AtomicTxnBatch, AtomicTxnBatchResult, BuilderHeartBeat, MultipleAtomicTxnBatch,
            MultipleAtomicTxnBatchResult, Packet,
            atomic_txn_batch_result::Result as BatchResultEnum,
        },
    },
    solana_keypair::Keypair,
    solana_signature::Signature,
    std::{
        str::FromStr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
    tempfile::tempdir,
    tokio::sync::{mpsc, oneshot, watch},
    tonic::{Request, Response, Status},
};

// ---------------------------------------------------------------------------
// 1. Mock BAM Node Server Implementation
// ---------------------------------------------------------------------------

struct MockBamNode {
    challenge: String,
    auth_verified: Arc<AtomicBool>,
    received_batch_results: Arc<Mutex<Vec<AtomicTxnBatchResult>>>,
}

#[tonic::async_trait]
impl BamNodeApi for MockBamNode {
    async fn get_auth_challenge(
        &self,
        _request: Request<AuthChallengeRequest>,
    ) -> Result<Response<AuthChallengeResponse>, Status> {
        Ok(Response::new(AuthChallengeResponse {
            challenge_to_sign: self.challenge.clone(),
        }))
    }

    async fn get_builder_config(
        &self,
        _request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        Ok(Response::new(ConfigResponse::default()))
    }

    type InitSchedulerStreamStream = tokio_stream::wrappers::ReceiverStream<Result<SchedulerResponse, Status>>;

    async fn init_scheduler_stream(
        &self,
        request: Request<tonic::Streaming<SchedulerMessage>>,
    ) -> Result<Response<Self::InitSchedulerStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (out_tx, out_rx) = mpsc::channel(16);

        // First message must be AuthProof
        let first_msg = inbound
            .message()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::invalid_argument("Missing first message"))?;

        if let Some(scheduler_message::VersionedMsg::V0(SchedulerMessageV0 {
            msg: Some(Msg::AuthProof(proof)),
        })) = first_msg.versioned_msg
        {
            let to_sign = BamClient::labeled_bytes(proof.challenge_to_sign.as_bytes());
            let pubkey = solana_pubkey::Pubkey::from_str(&proof.validator_pubkey)
                .map_err(|_| Status::unauthenticated("Invalid pubkey base58"))?;
            let signature = Signature::from_str(&proof.signature)
                .map_err(|_| Status::unauthenticated("Invalid signature base58"))?;

            if signature.verify(&pubkey.to_bytes(), &to_sign) {
                self.auth_verified.store(true, Ordering::SeqCst);
            }
        }

        // Send a heartbeat acknowledgement
        let hb_resp = SchedulerResponse {
            versioned_msg: Some(
                jito_protos::proto::bam_api::scheduler_response::VersionedMsg::V0(
                    SchedulerResponseV0 {
                        resp: Some(Resp::HeartBeat(BuilderHeartBeat {
                            time_sent_microseconds: 123456,
                        })),
                    },
                ),
            ),
        };
        let _ = out_tx.send(Ok(hb_resp)).await;

        let results_sink = self.received_batch_results.clone();
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                if let Some(scheduler_message::VersionedMsg::V0(SchedulerMessageV0 {
                    msg: Some(Msg::MultipleAtomicTxnBatchResult(MultipleAtomicTxnBatchResult { results })),
                })) = msg.versioned_msg
                {
                    results_sink.lock().unwrap().extend(results);
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(out_rx)))
    }
}

// ---------------------------------------------------------------------------
// 2. End-to-End Integration Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bam_auth_challenge_and_streaming() {
    let challenge = "test-challenge-uuid-99".to_string();
    let auth_verified = Arc::new(AtomicBool::new(false));
    let mock_node = MockBamNode {
        challenge: challenge.clone(),
        auth_verified: auth_verified.clone(),
        received_batch_results: Arc::new(Mutex::new(Vec::new())),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(BamNodeApiServer::new(mock_node))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let identity = Arc::new(Keypair::new());
    let url = format!("http://127.0.0.1:{}", local_addr.port());
    let (_url_tx, url_rx) = watch::channel(Some(url));
    let (_id_tx, id_rx) = watch::channel(Some(identity));
    let (batch_tx, _batch_rx) = unbounded();
    let (_outbound_tx, outbound_rx) = mpsc::channel(100);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let client_handle = tokio::spawn(async move {
        BamClient::run(url_rx, id_rx, batch_tx, outbound_rx, shutdown_rx).await;
    });

    // Wait up to 2 seconds for client to authenticate with mock BAM node
    for _ in 0..20 {
        if auth_verified.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        auth_verified.load(Ordering::SeqCst),
        "BAM Client failed to sign and verify challenge proof"
    );

    let _ = shutdown_tx.send(());
    let _ = client_handle.await;
}

#[test]
fn test_end_to_end_scheduler_batch_pipeline() {
    let dir = tempdir().unwrap();
    let ipc_path = dir.path().join("scheduler_bindings.ipc");

    let mut server = Server::new(&ipc_path).unwrap();
    let server_handle = std::thread::spawn(move || {
        let mut server_session = server.accept().unwrap();

        // 1. Send progress message indicating LEADER_READY
        let progress = ProgressMessage {
            leader_state: LEADER_READY,
            current_slot_progress: 0,
            epoch: 1,
            current_slot: 100,
            next_leader_slot: 104,
            leader_range_end: 104,
            remaining_cost_units: 48_000_000,
            remaining_allocated_accounts_data_size: 10_000_000,
            latest_blockhash: [7; 32],
            target_bank_time_ms: 0,
        };
        server_session.progress_tracker.try_write(progress).unwrap();

        // 2. Wait for pack_to_worker message dispatched by jito-builder
        let mut received_msg: Option<PackToExecutionWorkerMessage> = None;
        for _ in 0..100 {
            if let Some(msg) = server_session.workers[0].pack_to_worker.try_read() {
                received_msg = Some(msg);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let pack_msg = received_msg.expect("Expected PackToExecutionWorkerMessage");
        assert_eq!(pack_msg.max_working_slot, 100);
        assert_ne!(
            pack_msg.flags & execution_message_flags::ALL_OR_NOTHING,
            0,
            "Must set ALL_OR_NOTHING flag"
        );
        assert_eq!(pack_msg.batch.num_transactions, 1);

        // 3. Simulate worker execution response by allocating ExecutionResponse in shared memory
        let resp = ExecutionResponse {
            execution_slot: 100,
            not_included_reason: not_included_reasons::NONE,
            cost_units: 5000,
            fee_payer_balance: 1_000_000,
        };
        let responses = [resp];
        let bytes = unsafe {
            core::slice::from_raw_parts(
                responses.as_ptr() as *const u8,
                core::mem::size_of_val(&responses),
            )
        };
        let ptr = server_session
            .tpu_to_pack
            .allocator
            .allocate(bytes.len() as u32)
            .unwrap();
        let responses_offset = unsafe {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), bytes.len());
            server_session.tpu_to_pack.allocator.offset(ptr)
        };

        let worker_response = ExecutionWorkerToPackMessage {
            batch: pack_msg.batch,
            processed_code: processed_codes::PROCESSED,
            responses: ExecutionResponseRegion {
                num_transaction_responses: 1,
                transaction_responses_offset: responses_offset,
            },
        };
        server_session.workers[0]
            .worker_to_pack
            .try_write(worker_response)
            .unwrap();

        (server_session, pack_msg)
    });

    // Client side: connect to IPC
    let client_session = ipc_connect(&ipc_path, 1).unwrap();
    let progress_tracker = ProgressTracker::new(client_session.progress_tracker);

    let (batch_tx, batch_rx) = unbounded();
    let (outbound_tx, mut outbound_rx) = mpsc::channel(100);
    let (_id_tx, id_rx) = watch::channel(Some(Arc::new(Keypair::new())));
    let (_tm_tx, tm_rx) = watch::channel(None);
    let config = Config {
        ledger: dir.path().to_path_buf(),
        admin_rpc_path: None,
        ipc_socket_path: Some(ipc_path.clone()),
        bam_url: None,
        bam_node_registry_url: None,
        keypair: None,
        num_workers: 1,
        block_builder: None,
        block_builder_commission: 0,
        tip_payment_program: None,
        tip_distribution_program: None,
        merkle_root_upload_authority: None,
        commission_bps: 0,
    };

    let mut engine = SchedulerEngine::new(
        client_session.allocator,
        progress_tracker,
        client_session.workers,
        batch_rx,
        outbound_tx,
        id_rx,
        tm_rx,
        config,
    );

    // Enqueue 1 batch from BAM
    let dummy_tx_bytes = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let batch = MultipleAtomicTxnBatch {
        batches: vec![AtomicTxnBatch {
            seq_id: 42,
            max_schedule_slot: 100,
            packets: vec![Packet {
                data: Bytes::from(dummy_tx_bytes),
                meta: None,
            }],
        }],
        mss_padding: Bytes::new(),
    };
    batch_tx.send(batch).unwrap();

    let (engine_shutdown_tx, engine_shutdown_rx) = oneshot::channel();
    let engine_thread = std::thread::spawn(move || {
        engine.run(engine_shutdown_rx);
    });

    // Check that outbound_rx receives the committed batch result!
    let outbound_res = outbound_rx.blocking_recv().expect("Expected OutboundMsg");
    match outbound_res {
        OutboundMsg::BatchResult(res) => {
            assert_eq!(res.seq_id, 42);
            match res.result {
                Some(BatchResultEnum::Committed(committed)) => {
                    assert_eq!(committed.transaction_results.len(), 1);
                    assert_eq!(committed.transaction_results[0].cus_consumed, 5000);
                    assert_eq!(committed.transaction_results[0].feepayer_balance_lamports, 1_000_000);
                    assert!(committed.transaction_results[0].execution_success);
                }
                _ => panic!("Expected committed batch result"),
            }
        }
        _ => panic!("Expected BatchResult outbound message"),
    }

    let _ = engine_shutdown_tx.send(());
    let _ = engine_thread.join();
    let _ = server_handle.join();
}
