//! BAM node discovery and latency ranking

use {
    anyhow::{Result, anyhow},
    chrono::{DateTime, Utc},
    futures::stream::{self, StreamExt},
    jito_protos::proto::bam_api::{ConfigRequest, bam_node_api_client::BamNodeApiClient},
    log::{info, warn},
    serde::{Deserialize, Deserializer},
    std::{
        net::{IpAddr, SocketAddr},
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{sync::watch, time::sleep},
    tonic::transport::Endpoint,
};

pub const MAX_NODE_RTT: Duration = Duration::from_millis(30);
const PROBE_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const PROBE_REQUEST_TIMEOUT: Duration = Duration::from_millis(60);
const PROBE_FANOUT: usize = 32;
const PROBE_SAMPLES: usize = 3;
const RESYNC_INTERVAL_LIVE: Duration = Duration::from_secs(60);
const RESYNC_INTERVAL_STALLED: Duration = Duration::from_secs(5);
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
pub struct ServedNodes {
    #[allow(dead_code)]
    #[serde(default, deserialize_with = "lenient_rfc3339")]
    pub generated_at: Option<DateTime<Utc>>,
    pub nodes: Vec<ServedNode>,
}

fn lenient_rfc3339<'de, D>(deserializer: D) -> std::result::Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)
        .unwrap_or_default()
        .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok())
        .map(|parsed| parsed.with_timezone(&Utc)))
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ServedNode {
    pub ip: IpAddr,
    pub grpc_port: u16,
    pub region: String,
}

impl ServedNode {
    pub fn url(&self) -> String {
        format!("https://{}", SocketAddr::new(self.ip, self.grpc_port))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RankedNode {
    pub url: String,
    pub region: String,
    pub rtt_us: u64,
}

pub struct BamDiscovery {
    http_client: reqwest::Client,
    registry_url: Option<String>,
    static_url: Option<String>,
}

impl BamDiscovery {
    pub fn new(static_url: Option<String>, registry_url: Option<String>) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .build()
            .map_err(|e| anyhow!("Failed to build HTTP client for discovery: {e}"))?;
        Ok(Self {
            http_client,
            registry_url,
            static_url,
        })
    }

    /// Run discovery loop and broadcast best node URL
    pub async fn run(self: Arc<Self>, best_node_tx: watch::Sender<Option<String>>) {
        if let Some(static_url) = &self.static_url {
            info!("Using static BAM node URL: {static_url}");
            let _ = best_node_tx.send(Some(static_url.clone()));
            return;
        }

        let Some(registry_url) = &self.registry_url else {
            warn!("No BAM URL or registry URL provided; BAM discovery idle");
            return;
        };

        info!("Starting BAM discovery loop for registry {registry_url}");
        loop {
            match self.fetch_and_rank_nodes(registry_url).await {
                Ok(ranked) if !ranked.is_empty() => {
                    let best = &ranked[0];
                    info!(
                        "Discovered {} usable BAM nodes (RTT <= {:?}). Best: {} ({}) with RTT {} us",
                        ranked.len(),
                        MAX_NODE_RTT,
                        best.url,
                        best.region,
                        best.rtt_us
                    );
                    let _ = best_node_tx.send(Some(best.url.clone()));
                    sleep(RESYNC_INTERVAL_LIVE).await;
                }
                Ok(_) => {
                    warn!("No BAM nodes meet the <= {:?} RTT threshold; retrying in {:?}", MAX_NODE_RTT, RESYNC_INTERVAL_STALLED);
                    sleep(RESYNC_INTERVAL_STALLED).await;
                }
                Err(e) => {
                    warn!("Failed to fetch/rank BAM nodes from registry: {e:#}; retrying in {:?}", RESYNC_INTERVAL_STALLED);
                    sleep(RESYNC_INTERVAL_STALLED).await;
                }
            }
        }
    }

    async fn fetch_and_rank_nodes(&self, registry_url: &str) -> Result<Vec<RankedNode>> {
        let resp = self
            .http_client
            .get(registry_url)
            .send()
            .await
            .map_err(|e| anyhow!("GET {registry_url}: {e}"))?;
        let served_nodes: ServedNodes = resp
            .json()
            .await
            .map_err(|e| anyhow!("JSON decode from {registry_url}: {e}"))?;

        if served_nodes.nodes.is_empty() {
            return Ok(Vec::new());
        }

        let unique = Self::unique_nodes(served_nodes.nodes);
        let ranked = self.probe_nodes(unique).await;
        Ok(ranked)
    }

    pub fn unique_nodes(mut nodes: Vec<ServedNode>) -> Vec<ServedNode> {
        nodes.sort_by_key(|node| (node.ip, node.grpc_port));
        nodes.dedup_by_key(|node| (node.ip, node.grpc_port));
        nodes
    }

    pub fn is_usable(node: &RankedNode) -> bool {
        Duration::from_micros(node.rtt_us) <= MAX_NODE_RTT
    }

    async fn probe_nodes(&self, nodes: Vec<ServedNode>) -> Vec<RankedNode> {
        let ranked: Vec<Option<RankedNode>> = stream::iter(nodes)
            .map(|node| async move {
                Self::probe_node(&node).await.map(|rtt_us| RankedNode {
                    url: node.url(),
                    region: node.region,
                    rtt_us,
                })
            })
            .buffer_unordered(PROBE_FANOUT)
            .collect()
            .await;

        let mut valid: Vec<RankedNode> = ranked.into_iter().flatten().filter(Self::is_usable).collect();
        valid.sort_by_key(|n| n.rtt_us);
        valid
    }

    async fn probe_node(node: &ServedNode) -> Option<u64> {
        let url_str = node.url();
        let endpoint = Endpoint::from_shared(url_str.clone())
            .ok()?
            .connect_timeout(PROBE_CONNECT_TIMEOUT)
            .timeout(PROBE_REQUEST_TIMEOUT);

        let channel = endpoint.connect().await.ok()?;
        let mut client = BamNodeApiClient::new(channel);

        let mut total_us = 0u64;
        for _ in 0..PROBE_SAMPLES {
            let start = Instant::now();
            let req = tonic::Request::new(ConfigRequest {});
            match client.get_builder_config(req).await {
                Ok(_) => {
                    let elapsed = start.elapsed();
                    if elapsed > MAX_NODE_RTT {
                        return None;
                    }
                    total_us = total_us.saturating_add(elapsed.as_micros() as u64);
                }
                Err(_) => return None,
            }
        }

        Some(total_us / PROBE_SAMPLES as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOLDEN_NODE: &str = r#"{"ip":"203.0.113.1","grpc_port":50056,"region":"fra"}"#;

    fn served_node(ip: &str) -> ServedNode {
        ServedNode {
            ip: ip.parse().unwrap(),
            grpc_port: 50056,
            region: "fra".to_string(),
        }
    }

    fn ranked_node(ip: &str, rtt_us: u64) -> RankedNode {
        RankedNode {
            url: format!("https://{ip}:50056"),
            region: "fra".to_string(),
            rtt_us,
        }
    }

    #[test]
    fn test_deserialize_ignores_unknown_fields() {
        let json = r#"{"ip":"203.0.113.1","grpc_port":50056,"region":"fra","added_later":true}"#;
        let node: ServedNode = serde_json::from_str(json).unwrap();
        assert_eq!(node.grpc_port, 50056);
    }

    #[test]
    fn test_deserialize_served_nodes() {
        let json = format!(r#"{{"generated_at":"2026-09-09T12:34:56Z","nodes":[{GOLDEN_NODE}]}}"#);
        let served: ServedNodes = serde_json::from_str(&json).unwrap();
        assert_eq!(
            served.generated_at,
            Some("2026-09-09T12:34:56Z".parse::<DateTime<Utc>>().unwrap()),
        );
        assert_eq!(served.nodes.len(), 1);
    }

    #[test]
    fn test_deserialize_tolerates_bad_generated_at() {
        for value in [r#""not-a-timestamp""#, "null", "1757419200"] {
            let json = format!(r#"{{"generated_at":{value},"nodes":[{GOLDEN_NODE}]}}"#);
            let served: ServedNodes = serde_json::from_str(&json).unwrap();
            assert_eq!(served.generated_at, None);
            assert_eq!(served.nodes.len(), 1);
        }
    }

    #[test]
    fn test_deserialize_tolerates_absent_generated_at() {
        let json = format!(r#"{{"nodes":[{GOLDEN_NODE}]}}"#);
        let served: ServedNodes = serde_json::from_str(&json).unwrap();
        assert_eq!(served.generated_at, None);
    }

    #[test]
    fn test_deserialize_empty_node_list() {
        let json = r#"{"generated_at":"2026-09-09T12:34:56Z","nodes":[]}"#;
        let served: ServedNodes = serde_json::from_str(json).unwrap();
        assert!(served.nodes.is_empty());
    }

    #[test]
    fn test_url_ipv4() {
        let node: ServedNode = serde_json::from_str(GOLDEN_NODE).unwrap();
        assert_eq!(node.url(), "https://203.0.113.1:50056");
    }

    #[test]
    fn test_unique_nodes_ignores_order() {
        let a = served_node("203.0.113.1");
        let b = served_node("203.0.113.2");
        assert_eq!(
            BamDiscovery::unique_nodes(vec![b.clone(), a.clone()]),
            BamDiscovery::unique_nodes(vec![a, b])
        );
    }

    #[test]
    fn test_unique_nodes_drops_duplicate_endpoints() {
        let a = served_node("203.0.113.1");
        let b = served_node("203.0.113.2");
        let relabeled_a = ServedNode {
            region: "ams".to_string(),
            ..a.clone()
        };
        assert_eq!(
            BamDiscovery::unique_nodes(vec![a.clone(), b.clone(), relabeled_a, a.clone()]),
            vec![a, b]
        );
    }

    #[test]
    fn test_is_usable() {
        assert!(BamDiscovery::is_usable(&ranked_node("203.0.113.1", 30_000)));
        assert!(!BamDiscovery::is_usable(&ranked_node("203.0.113.1", 30_001)));
    }
}
