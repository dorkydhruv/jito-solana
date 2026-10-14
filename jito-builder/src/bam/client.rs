//! BAM gRPC client for bidirectional streaming with the BAM Node

use {
    anyhow::{Result, anyhow},
    jito_protos::proto::{
        bam_api::{
            AuthChallengeRequest, SchedulerMessage, SchedulerMessageV0, SchedulerResponse,
            bam_node_api_client::BamNodeApiClient, scheduler_message, scheduler_message_v0::Msg,
            scheduler_response, scheduler_response_v0::Resp,
        },
        bam_types::{
            AuthProof, MultipleAtomicTxnBatch, MultipleAtomicTxnBatchResult, Ping, Pong,
            ValidatorHeartBeat,
        },
    },
    log::{debug, error, info, warn},
    solana_keypair::Keypair,
    solana_signer::Signer,
    std::{
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        sync::{mpsc, oneshot, watch},
        time::{interval, sleep, timeout},
    },
    tokio_stream::wrappers::ReceiverStream,
    tonic::transport::Endpoint,
};

pub const AUTH_LABEL: &[u8] = b"X_OFF_CHAIN_JITO_BAM_V1\0";
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const NETWORK_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const VALIDATOR_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const MAX_DURATION_BETWEEN_NODE_HEARTBEATS: Duration = Duration::from_secs(6);
const OUTBOUND_QUEUE_CAPACITY: usize = 10_000;

pub enum OutboundMsg {
    LeaderState(jito_protos::proto::bam_types::LeaderState),
    BatchResult(jito_protos::proto::bam_types::AtomicTxnBatchResult),
}

pub struct BamClient;

impl BamClient {
    pub async fn run(
        url_rx: watch::Receiver<Option<String>>,
        identity_rx: watch::Receiver<Option<Arc<Keypair>>>,
        batch_tx: crossbeam_channel::Sender<MultipleAtomicTxnBatch>,
        outbound_rx: mpsc::Receiver<OutboundMsg>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let mut outbound_rx = Some(outbound_rx);
        loop {
            // Wait for both URL and Identity keypair
            let (url, identity) = match Self::wait_for_prerequisites(&url_rx, &identity_rx).await {
                Ok(tuple) => tuple,
                Err(_) => {
                    info!("BAM client shutting down before prerequisites were met");
                    return;
                }
            };

            info!("Connecting to BAM node at {url}...");
            let curr_outbound = outbound_rx.take().expect("Outbound receiver must exist");

            tokio::select! {
                _ = &mut shutdown_rx => {
                    info!("BAM client shutdown requested");
                    return;
                }
                res = Self::connect_and_stream(
                    &url,
                    identity,
                    &batch_tx,
                    curr_outbound,
                ) => {
                    match res {
                        Ok(returned_rx) => {
                            outbound_rx = Some(returned_rx);
                            warn!("BAM stream ended gracefully; reconnecting in 1s...");
                        }
                        Err((returned_rx, err)) => {
                            outbound_rx = Some(returned_rx);
                            error!("BAM connection error: {err:#}; reconnecting in 1s...");
                        }
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn wait_for_prerequisites(
        url_rx: &watch::Receiver<Option<String>>,
        identity_rx: &watch::Receiver<Option<Arc<Keypair>>>,
    ) -> Result<(String, Arc<Keypair>)> {
        let mut url_watch = url_rx.clone();
        let mut id_watch = identity_rx.clone();

        loop {
            let url_opt = url_watch.borrow().clone();
            let id_opt = id_watch.borrow().clone();

            if let (Some(url), Some(id)) = (url_opt, id_opt) {
                return Ok((url, id));
            }

            tokio::select! {
                res = url_watch.changed() => {
                    if res.is_err() {
                        return Err(anyhow!("URL watch closed"));
                    }
                }
                res = id_watch.changed() => {
                    if res.is_err() {
                        return Err(anyhow!("Identity watch closed"));
                    }
                }
            }
        }
    }

    async fn connect_and_stream(
        url: &str,
        identity: Arc<Keypair>,
        batch_tx: &crossbeam_channel::Sender<MultipleAtomicTxnBatch>,
        mut outbound_rx: mpsc::Receiver<OutboundMsg>,
    ) -> Result<mpsc::Receiver<OutboundMsg>, (mpsc::Receiver<OutboundMsg>, anyhow::Error)> {
        let endpoint = match Endpoint::from_shared(url.to_string()) {
            Ok(ep) => ep
                .connect_timeout(CONNECTION_TIMEOUT)
                .timeout(NETWORK_REQUEST_TIMEOUT),
            Err(e) => return Err((outbound_rx, anyhow!("Invalid BAM endpoint: {e}"))),
        };

        let channel = match timeout(CONNECTION_TIMEOUT, endpoint.connect()).await {
            Ok(Ok(ch)) => ch,
            Ok(Err(e)) => return Err((outbound_rx, anyhow!("Failed to connect to BAM node: {e}"))),
            Err(_) => return Err((outbound_rx, anyhow!("Connection to BAM node timed out"))),
        };

        let mut client = BamNodeApiClient::new(channel);

        // 1. Get Auth Challenge
        let challenge = match timeout(
            NETWORK_REQUEST_TIMEOUT,
            client.get_auth_challenge(tonic::Request::new(AuthChallengeRequest {})),
        )
        .await
        {
            Ok(Ok(resp)) => resp.into_inner().challenge_to_sign,
            Ok(Err(e)) => return Err((outbound_rx, anyhow!("GetAuthChallenge failed: {e}"))),
            Err(_) => return Err((outbound_rx, anyhow!("GetAuthChallenge timed out"))),
        };

        // 2. Sign Challenge
        let msg_to_sign = Self::labeled_bytes(challenge.as_bytes());
        let sig = identity.sign_message(&msg_to_sign);
        let auth_proof = AuthProof {
            challenge_to_sign: challenge,
            validator_pubkey: identity.pubkey().to_string(),
            signature: sig.to_string(),
        };

        // 3. Setup Bidirectional Streaming
        let (stream_tx, stream_rx) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);

        // Send auth proof as first message
        let first_msg = SchedulerMessage {
            versioned_msg: Some(scheduler_message::VersionedMsg::V0(SchedulerMessageV0 {
                msg: Some(Msg::AuthProof(auth_proof)),
            })),
        };
        if let Err(e) = stream_tx.send(first_msg).await {
            return Err((outbound_rx, anyhow!("Failed to enqueue auth proof: {e}")));
        }

        let inbound = match timeout(
            NETWORK_REQUEST_TIMEOUT,
            client.init_scheduler_stream(tonic::Request::new(ReceiverStream::new(stream_rx))),
        )
        .await
        {
            Ok(Ok(resp)) => resp.into_inner(),
            Ok(Err(e)) => return Err((outbound_rx, anyhow!("InitSchedulerStream failed: {e}"))),
            Err(_) => return Err((outbound_rx, anyhow!("InitSchedulerStream timed out"))),
        };

        info!("BAM scheduler stream established successfully with {url}");

        // Stream loop
        match Self::stream_loop(inbound, stream_tx, &mut outbound_rx, batch_tx).await {
            Ok(()) => Ok(outbound_rx),
            Err(e) => Err((outbound_rx, e)),
        }
    }

    async fn stream_loop(
        mut inbound: tonic::Streaming<SchedulerResponse>,
        stream_tx: mpsc::Sender<SchedulerMessage>,
        outbound_rx: &mut mpsc::Receiver<OutboundMsg>,
        batch_tx: &crossbeam_channel::Sender<MultipleAtomicTxnBatch>,
    ) -> Result<()> {
        let mut heartbeat_timer = interval(VALIDATOR_HEARTBEAT_INTERVAL);
        let mut last_node_heartbeat = Instant::now();
        let mut pending_results = Vec::with_capacity(32);

        loop {
            tokio::select! {
                _ = heartbeat_timer.tick() => {
                    if last_node_heartbeat.elapsed() > MAX_DURATION_BETWEEN_NODE_HEARTBEATS {
                        return Err(anyhow!("BAM node heartbeat timeout (> {:?})", MAX_DURATION_BETWEEN_NODE_HEARTBEATS));
                    }
                    let now_us = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as u64;
                    let hb_msg = SchedulerMessage {
                        versioned_msg: Some(scheduler_message::VersionedMsg::V0(SchedulerMessageV0 {
                            msg: Some(Msg::HeartBeat(ValidatorHeartBeat {
                                time_sent_microseconds: now_us,
                            })),
                        })),
                    };
                    if stream_tx.send(hb_msg).await.is_err() {
                        return Err(anyhow!("Failed to send validator heartbeat; stream broken"));
                    }
                }

                outbound_item = outbound_rx.recv() => {
                    let Some(item) = outbound_item else {
                        return Err(anyhow!("Outbound channel closed"));
                    };
                    match item {
                        OutboundMsg::LeaderState(ls) => {
                            let msg = SchedulerMessage {
                                versioned_msg: Some(scheduler_message::VersionedMsg::V0(SchedulerMessageV0 {
                                    msg: Some(Msg::LeaderState(ls)),
                                })),
                            };
                            if stream_tx.send(msg).await.is_err() {
                                return Err(anyhow!("Failed to send LeaderState"));
                            }
                        }
                        OutboundMsg::BatchResult(res) => {
                            pending_results.push(res);
                            // Batch up results or send immediately if channel is empty
                            if pending_results.len() >= 16 || outbound_rx.is_empty() {
                                let results = std::mem::take(&mut pending_results);
                                let msg = SchedulerMessage {
                                    versioned_msg: Some(scheduler_message::VersionedMsg::V0(SchedulerMessageV0 {
                                        msg: Some(Msg::MultipleAtomicTxnBatchResult(
                                            MultipleAtomicTxnBatchResult { results },
                                        )),
                                    })),
                                };
                                if stream_tx.send(msg).await.is_err() {
                                    return Err(anyhow!("Failed to send batch results"));
                                }
                            }
                        }
                    }
                }

                resp_opt = inbound.message() => {
                    let resp = resp_opt.map_err(|e| anyhow!("Inbound stream error: {e}"))?;
                    let Some(resp) = resp else {
                        return Err(anyhow!("Inbound stream closed by server"));
                    };

                    let Some(scheduler_response::VersionedMsg::V0(v0)) = resp.versioned_msg else {
                        continue;
                    };

                    match v0.resp {
                        Some(Resp::MultipleAtomicTxnBatch(batch)) => {
                            debug!("Received MultipleAtomicTxnBatch with {} batches", batch.batches.len());
                            if let Err(e) = batch_tx.send(batch) {
                                error!("Failed to forward BAM batch to scheduler: {e}");
                            }
                        }
                        Some(Resp::HeartBeat(_hb)) => {
                            last_node_heartbeat = Instant::now();
                        }
                        Some(Resp::Ping(Ping { id })) => {
                            let pong_msg = SchedulerMessage {
                                versioned_msg: Some(scheduler_message::VersionedMsg::V0(SchedulerMessageV0 {
                                    msg: Some(Msg::Pong(Pong { id })),
                                })),
                            };
                            let _ = stream_tx.send(pong_msg).await;
                        }
                        None => {}
                    }
                }
            }
        }
    }

    /// Construct labeled bytes that must be signed for BAM authentication
    pub fn labeled_bytes(challenge: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(AUTH_LABEL.len() + challenge.len());
        v.extend_from_slice(AUTH_LABEL);
        v.extend_from_slice(challenge);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_labeled_bytes_format() {
        let challenge = b"test-challenge-12345";
        let labeled = BamClient::labeled_bytes(challenge);
        assert!(labeled.starts_with(AUTH_LABEL));
        assert_eq!(&labeled[AUTH_LABEL.len()..], challenge);
    }

    #[test]
    fn test_auth_proof_signing_and_verification() {
        let keypair = Keypair::new();
        let challenge = "test-challenge-12345";
        let msg_to_sign = BamClient::labeled_bytes(challenge.as_bytes());
        let sig = keypair.sign_message(&msg_to_sign);
        assert!(sig.verify(&keypair.pubkey().to_bytes(), &msg_to_sign));
    }
}
