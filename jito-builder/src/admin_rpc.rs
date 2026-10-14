//! Admin RPC client for communicating with the Agave validator's admin.rpc socket

use {
    crate::config::Config,
    anyhow::{Result, anyhow},
    jsonrpc_core::Params,
    jsonrpc_core_client::{RawClient, RpcError, transports::ipc},
    log::{error, info, warn},
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        path::Path,
        sync::Arc,
        time::Duration,
    },
    tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig},
    tokio::{
        sync::{oneshot, watch},
        time::{interval, sleep},
    },
};

const CONNECT_BACKOFF: Duration = Duration::from_secs(5);
const IDENTITY_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub async fn run(
    config: Config,
    identity_tx: watch::Sender<Option<Arc<Keypair>>>,
    tip_manager_tx: watch::Sender<Option<Arc<TipManager>>>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let admin_rpc_path = config.resolved_admin_rpc_path();
    let mut last_vote_account: Option<Pubkey> = None;

    // If an explicit keypair was provided via CLI, load it immediately
    if let Some(keypair_path) = &config.keypair {
        match solana_keypair::read_keypair_file(keypair_path) {
            Ok(kp) => {
                info!("Loaded identity keypair from file: {}", kp.pubkey());
                let _ = identity_tx.send(Some(Arc::new(kp)));
            }
            Err(e) => {
                error!("Failed to read keypair file {}: {e}", keypair_path.display());
            }
        }
    }

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                info!("Admin RPC runner shutting down");
                return;
            }
            res = connect_and_watch(
                &admin_rpc_path,
                &config,
                &identity_tx,
                &tip_manager_tx,
                &mut last_vote_account,
            ) => {
                if let Err(e) = res {
                    warn!("Admin RPC connection lost or failed ({e:#}); retrying in {CONNECT_BACKOFF:?}...");
                    tokio::select! {
                        _ = &mut shutdown_rx => return,
                        _ = sleep(CONNECT_BACKOFF) => {}
                    }
                }
            }
        }
    }
}

async fn connect_and_watch(
    admin_rpc_path: &Path,
    config: &Config,
    identity_tx: &watch::Sender<Option<Arc<Keypair>>>,
    tip_manager_tx: &watch::Sender<Option<Arc<TipManager>>>,
    last_vote_account: &mut Option<Pubkey>,
) -> Result<()> {
    info!("Connecting to admin RPC at {}", admin_rpc_path.display());
    let client = ipc::connect(admin_rpc_path)
        .await
        .map_err(|e| anyhow!("Failed to connect to admin RPC at {}: {e}", admin_rpc_path.display()))?;

    // Initial fetch
    let (identity, vote_account) = fetch_identity(&client)
        .await
        .map_err(|e| anyhow!("Failed to fetch identity: {e}"))?;
    update_identity(config, identity_tx, identity);
    update_tip_manager(config, tip_manager_tx, last_vote_account, vote_account);

    let mut poll_timer = interval(IDENTITY_POLL_INTERVAL);
    loop {
        poll_timer.tick().await;
        let (identity, vote_account) = fetch_identity(&client)
            .await
            .map_err(|e| anyhow!("Failed to poll identity: {e}"))?;
        update_identity(config, identity_tx, identity);
        update_tip_manager(config, tip_manager_tx, last_vote_account, vote_account);
    }
}

async fn fetch_identity(client: &RawClient) -> Result<(Keypair, Pubkey), RpcError> {
    let identity_bytes: Vec<u8> =
        serde_json::from_value(client.call_method("getIdentity", Params::None).await?)
            .map_err(|e| RpcError::ParseError(e.to_string(), Box::new(e)))?;
    let identity = Keypair::try_from(identity_bytes.as_slice())
        .map_err(|e| RpcError::ParseError(e.to_string(), Box::new(std::io::Error::other(e.to_string()))))?;

    let vote_account: Pubkey =
        serde_json::from_value(client.call_method("getVoteAccount", Params::None).await?)
            .map_err(|e| RpcError::ParseError(e.to_string(), Box::new(e)))?;

    Ok((identity, vote_account))
}

fn update_identity(
    config: &Config,
    identity_tx: &watch::Sender<Option<Arc<Keypair>>>,
    identity: Keypair,
) {
    if config.keypair.is_none() {
        let current_pubkey = identity_tx.borrow().as_ref().map(|k| k.pubkey());
        if current_pubkey != Some(identity.pubkey()) {
            info!("Validator identity updated from admin RPC: {}", identity.pubkey());
            let _ = identity_tx.send(Some(Arc::new(identity)));
        }
    }
}

fn update_tip_manager(
    config: &Config,
    tip_manager_tx: &watch::Sender<Option<Arc<TipManager>>>,
    last_vote_account: &mut Option<Pubkey>,
    vote_account: Pubkey,
) {
    if *last_vote_account != Some(vote_account) {
        info!("Validator vote account updated from admin RPC: {vote_account}");
        *last_vote_account = Some(vote_account);

        let tip_payment_program = config.tip_payment_program.unwrap_or_else(Pubkey::new_unique);
        let tip_distribution_program = config
            .tip_distribution_program
            .unwrap_or_else(Pubkey::new_unique);
        let merkle_root_upload_authority = config
            .merkle_root_upload_authority
            .unwrap_or_else(Pubkey::new_unique);

        let tm = Arc::new(TipManager::new(TipManagerConfig {
            tip_payment_program_id: tip_payment_program,
            tip_distribution_program_id: tip_distribution_program,
            tip_distribution_account_config: TipDistributionAccountConfig {
                merkle_root_upload_authority,
                vote_account,
                commission_bps: config.commission_bps,
            },
        }));

        let _ = tip_manager_tx.send(Some(tm));
    }
}
