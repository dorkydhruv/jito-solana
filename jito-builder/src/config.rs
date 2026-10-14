//! CLI configuration for the jito-builder daemon

use {
    agave_scheduler_handshake::MAX_WORKERS,
    clap::{Parser, builder::RangedU64ValueParser},
    solana_pubkey::Pubkey,
    std::path::PathBuf,
};

#[derive(Debug, Clone, Parser)]
#[command(
    name = "jito-builder",
    about = "Jito BAM block builder & scheduler daemon for Agave validator"
)]
pub struct Config {
    /// Ledger directory (used to find admin.rpc and scheduler IPC socket if not explicitly provided)
    #[arg(short = 'l', long, value_name = "DIR", default_value = "ledger")]
    pub ledger: PathBuf,

    /// Agave admin RPC socket path (defaults to <LEDGER>/admin.rpc)
    #[arg(long, value_name = "PATH")]
    pub admin_rpc_path: Option<PathBuf>,

    /// Agave scheduler IPC socket path (defaults to <LEDGER>/scheduler.sock or /tmp/scheduler.sock)
    #[arg(long, value_name = "PATH")]
    pub ipc_socket_path: Option<PathBuf>,

    /// Direct BAM node gRPC endpoint (e.g. https://bam.mainnet.jito.network:443)
    #[arg(long, value_name = "URL")]
    pub bam_url: Option<String>,

    /// Dynamic BAM node registry JSON endpoint (e.g. https://bam.jito.network/nodes.json)
    #[arg(long, value_name = "URL")]
    pub bam_node_registry_url: Option<String>,

    /// Validator identity keypair path (if omitted, fetched from admin.rpc getIdentity)
    #[arg(long, value_name = "PATH")]
    pub keypair: Option<PathBuf>,

    /// Number of Agave scheduler workers (default: 8)
    #[arg(
        long,
        default_value_t = 8,
        value_parser = RangedU64ValueParser::<usize>::new().range(1..=MAX_WORKERS as u64)
    )]
    pub num_workers: usize,

    /// Block builder cut receiver pubkey override
    #[arg(long, value_parser = parse_pubkey)]
    pub block_builder: Option<Pubkey>,

    /// Block builder commission BPS (0..=10000)
    #[arg(long, default_value_t = 0, value_parser = RangedU64ValueParser::<u64>::new().range(..=10000))]
    pub block_builder_commission: u64,

    /// Tip payment program pubkey
    #[arg(long, value_parser = parse_pubkey)]
    pub tip_payment_program: Option<Pubkey>,

    /// Tip distribution program pubkey
    #[arg(long, value_parser = parse_pubkey)]
    pub tip_distribution_program: Option<Pubkey>,

    /// Merkle root upload authority pubkey
    #[arg(long, value_parser = parse_pubkey)]
    pub merkle_root_upload_authority: Option<Pubkey>,

    /// Validator tip commission BPS (0..=10000)
    #[arg(long, default_value_t = 0, value_parser = RangedU64ValueParser::<u16>::new().range(..=10000))]
    pub commission_bps: u16,
}

impl Config {
    pub fn resolved_admin_rpc_path(&self) -> PathBuf {
        self.admin_rpc_path
            .clone()
            .unwrap_or_else(|| self.ledger.join("admin.rpc"))
    }

    pub fn resolved_ipc_socket_path(&self) -> PathBuf {
        self.ipc_socket_path.clone().unwrap_or_else(|| {
            let candidate1 = self.ledger.join("scheduler_bindings.ipc");
            if candidate1.exists() {
                return candidate1;
            }
            let candidate2 = self.ledger.join("scheduler.sock");
            if candidate2.exists() {
                return candidate2;
            }
            candidate1
        })
    }
}

fn parse_pubkey(s: &str) -> Result<Pubkey, String> {
    s.parse::<Pubkey>()
        .map_err(|e| format!("invalid pubkey '{s}': {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let cfg = Config::parse_from(["jito-builder"]);
        assert_eq!(cfg.num_workers, 8);
        assert_eq!(cfg.ledger, PathBuf::from("ledger"));
        assert_eq!(cfg.resolved_admin_rpc_path(), PathBuf::from("ledger/admin.rpc"));
    }

    #[test]
    fn test_config_flags() {
        let cfg = Config::parse_from([
            "jito-builder",
            "--num-workers", "4",
            "--bam-url", "https://127.0.0.1:443",
            "--commission-bps", "500",
        ]);
        assert_eq!(cfg.num_workers, 4);
        assert_eq!(cfg.bam_url.as_deref(), Some("https://127.0.0.1:443"));
        assert_eq!(cfg.commission_bps, 500);
    }
}
