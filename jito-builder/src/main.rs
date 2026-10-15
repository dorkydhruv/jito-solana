use {
    clap::Parser,
    crossbeam_channel::unbounded,
    jito_builder::{
        admin_rpc,
        bam::{client::BamClient, discovery::BamDiscovery},
        config::Config,
        scheduler::{engine::SchedulerEngine, ipc::connect as ipc_connect, progress::ProgressTracker},
    },
    log::{info, trace},
    std::{sync::Arc, time::Duration},
    tokio::{
        signal,
        sync::{mpsc, oneshot, watch},
        time::sleep,
    },
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    agave_logger::setup();
    let config = Config::parse();

    info!("Starting jito-builder daemon...");
    info!("Configuration: {:?}", config);

    let (admin_shutdown_tx, admin_shutdown_rx) = oneshot::channel();
    let (bam_shutdown_tx, bam_shutdown_rx) = oneshot::channel();
    let (engine_shutdown_tx, engine_shutdown_rx) = oneshot::channel();

    let (url_tx, url_rx) = watch::channel(config.bam_url.clone());
    let (identity_tx, identity_rx) = watch::channel(None);
    let (tip_manager_tx, tip_manager_rx) = watch::channel(None);
    let (batch_tx, batch_rx) = unbounded();
    let (outbound_tx, outbound_rx) = mpsc::channel(10_000);

    // 1. Start Admin RPC client
    let admin_config = config.clone();
    tokio::spawn(admin_rpc::run(
        admin_config,
        identity_tx,
        tip_manager_tx,
        admin_shutdown_rx,
    ));

    // 2. Start BAM discovery
    let discovery = Arc::new(BamDiscovery::new(
        config.bam_url.clone(),
        config.bam_node_registry_url.clone(),
    )?);
    tokio::spawn(discovery.run(url_tx));

    // 3. Start BAM gRPC client
    tokio::spawn(BamClient::run(
        url_rx,
        identity_rx.clone(),
        batch_tx,
        outbound_rx,
        bam_shutdown_rx,
    ));

    // 4. Connect to Agave scheduler IPC
    let ipc_path = config.resolved_ipc_socket_path();
    let num_workers = config.num_workers;

    info!("Waiting for Agave validator scheduler IPC at {}...", ipc_path.display());
    let session = loop {
        match ipc_connect(&ipc_path, num_workers) {
            Ok(s) => {
                info!("Successfully established IPC handshake with Agave validator");
                break s;
            }
            Err(e) => {
                trace!("IPC connection attempt failed: {e}; retrying in 1s...");
                sleep(Duration::from_secs(1)).await;
            }
        }
    };

    let progress = ProgressTracker::new(session.progress_tracker);
    let mut engine = SchedulerEngine::new(
        session.allocator,
        progress,
        session.workers,
        batch_rx,
        outbound_tx,
        identity_rx,
        tip_manager_rx,
        config.clone(),
    );

    // Run scheduler engine on dedicated thread for low-latency scheduling
    let engine_handle = std::thread::Builder::new()
        .name("jito-builder-sched".into())
        .spawn(move || {
            engine.run(engine_shutdown_rx);
        })?;

    // Wait for shutdown signal (Ctrl+C or SIGTERM)
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C; shutting down jito-builder...");
        }
    }

    let _ = admin_shutdown_tx.send(());
    let _ = bam_shutdown_tx.send(());
    let _ = engine_shutdown_tx.send(());

    let _ = engine_handle.join();
    info!("jito-builder terminated cleanly");

    Ok(())
}
