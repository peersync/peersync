use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Error, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use log::{warn, debug, info};
use tokio::{sync::oneshot::{self, error::TryRecvError, Receiver}, time::timeout};

use crate::{
    dht::method::announce,
    multicast::{find_peer, is_peer_local},
    node::{Key, Node},
    parallel_download::{local_storage::Range, protocol::get_peer_block_status, LSCommand},
    system::System,
    tracker::{announce_to_trackers, TrackerListCommand},
    util::*,
};

const LAMBDA: f64 = 1.0;
const BETA_1: f64 = 0.8;
const BETA_2: f64 = 0.2;

#[derive(Clone, Debug)]
pub struct PeerInformation {
    pub node: Node,
    pub score: f64,
    pub ranges: Vec<Range>,
    pub all_layers: Vec<Key>,
}

impl Hash for PeerInformation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node.hash(state);
    }
}

impl PartialEq for PeerInformation {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}

impl Eq for PeerInformation {}

pub struct PeerAggregator {
    target_key: Key,
    pub peers: Arc<Mutex<HashSet<PeerInformation>>>,
    system: Arc<System>,
}

impl From<Node> for PeerInformation {
    fn from(value: Node) -> Self {
        Self {
            node: value,
            score: 100.0,
            ranges: vec![],
            all_layers: vec![],
        }
    }
}

impl PeerAggregator {
    // timeout is in ms
    pub async fn init(
        key: Key,
        timeout: u64,
        system: Arc<System>,
    ) -> Result<Self> {
        let pa = Self {
            target_key: key,
            peers: Arc::new(Mutex::new(HashSet::new())),
            system: Arc::clone(&system),
        };

        pa.exec_impl(timeout / 2).await;
        pa.update_impl(timeout / 2, system).await?;

        let mut text = String::from("Peer aggregator got ");
        let peers = pa.peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.clone();
        text.push_str(&format!("{} peer(s):", peers.len()));
        for peer in peers {
            text.push_str(&format!(" {}", peer.node.name));
        }
        text.push_str(&format!(". "));
        info!("{text}");

        Ok(pa)
    }

    pub async fn go(self, interval: u64, mut terminator: Receiver<()>, system: Arc<System>) -> Result<()> {
        loop {
            self.exec_impl(interval).await;

            let system = Arc::clone(&system);
            self.update_impl(interval, system).await?;

            match terminator.try_recv() {
                Err(TryRecvError::Empty) => {},
                _ => break,
            }
        }

        Ok(())
    }

    async fn update_impl(&self, timeout_ms: u64, system: Arc<System>) -> Result<()> {
        let peers = self.peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.clone();

        let mut futures = vec![];

        for peer in peers {
            let orig_peer = peer.clone();
            let system_local = Arc::clone(&system);
            let system_block = Arc::clone(&system);
            let target = self.target_key.clone();
            let handle = timeout(
                Duration::from_millis(timeout_ms),
                tokio::spawn(async move {
                let key = peer.node.key.clone();
                let is_local_handler = tokio::spawn(is_peer_local(key, system_local));

                let (ranges, layers) = get_peer_block_status(
                    &peer.node,
                    target,
                    system_block,
                ).await?;
                let is_local = is_local_handler.await?;
                
                Ok::<(Vec<Range>, Vec<Key>, bool), Error>((ranges, layers, is_local))
            }));

            futures.push((handle, orig_peer));
        }

        // arrange according to networking conditions
        let mut peers = vec![];
        for (handle, orig_peer) in futures {
            let peer_new = if let Ok(Ok(Ok((ranges, layers, is_local)))) = handle.await {
                let score = if is_local {
                    100.0
                } else {
                    let (tx, rx) = oneshot::channel();
                    let speed_diff = {
                        let command = LSCommand::GetAvgSpeedDiff(orig_peer.node.key.clone(), tx);
                        system.local_storage_command_tx.send(command)?;
                        rx.await?
                    };

                    speed_diff // will be normalized later
                };

                PeerInformation {
                    node: orig_peer.node,
                    score,
                    ranges,
                    all_layers: layers,
                }
            } else {
                orig_peer
            };

            peers.push(peer_new);
        }

        // normalize network-based scores
        let scores: Vec<_> = peers.iter().map(|p| p.score).collect();
        let scores = normalize(scores);
        let peers: HashSet<_> = peers
            .into_iter()
            .zip(scores)
            .map(|(mut p, s)| {
                if s.is_nan() {
                    panic!("first")
                }
                p.score = s;
                p
            })
            .collect();

        // arrange according to image popularity
        let image_count = {
            let mut count = HashMap::new();
            let peers = self.peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.clone();
            for pi in peers {
                for layer in pi.all_layers {
                    let count = count.entry(layer).or_insert(0);
                    *count += 1;
                }
            }
            count
        };

        let image_freq: HashMap<_, _> = {
            let total_images: usize = image_count.iter().map(|(_, c)| c).sum();
            let total_images = total_images as f64;
            let freq: Vec<_> = image_count
                .iter()
                .map(|(_, c)| {
                    if total_images == 0.0 {
                        0.0
                    } else {
                        *c as f64 / total_images
                    }
                })
                .collect();
            let freq = normalize(freq);
            image_count.into_iter().zip(freq).map(|((k, _), f)| (k, f)).collect()
        };

        // the more popular an image is, the less its rarity
        let image_rarity: HashMap<_, _> = image_freq
            .into_iter()
            .map(|(k, f)| (k, (-LAMBDA * f).exp()))
            .collect();

        let popularity_scores: HashMap<_, _> = {
            let peers = self.peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.clone();
            peers
                .into_iter()
                .map(|peer| {
                    // we computed the rarity information before by iterating everything, so unwrapping is safe
                    let sum: f64 = peer.all_layers.iter().map(|l| image_rarity.get(l).unwrap()).sum();
                    let avg = if peer.all_layers.len() != 0 {
                        sum / peer.all_layers.len() as f64
                    } else {
                        0.0
                    };
                    // we prefer peers with LOWER avg value
                    (peer.node.key, 100.0 - (1.0 - avg))
                })
                .collect()
        };

        let peers: HashSet<_> = peers
            .into_iter()
            .map(|mut p| {
                let pop_score = popularity_scores.get(&p.node.key).unwrap();
                let score = BETA_1 * p.score + BETA_2 * pop_score;
                p.score = score;
                p
            })
            .collect();

        for peer in &peers {
            let score = peer.score;
            if !score.is_normal() || score < 0.0 {
                warn!("Bad score for {}: {score}. ", peer.node.name);
            }
        }

        *self.peers.lock().map_err(|_| anyhow!("failed to acquire lock"))? = peers;

        Ok(())
    }

    async fn exec_impl(&self, timeout: u64) {
        let mut futures = FuturesUnordered::new();

        let system_dht = Arc::clone(&self.system);
        let peers_dht = Arc::clone(&self.peers);
        let key = self.target_key.clone();
        futures.push(tokio::spawn(get_peers_from_dht_wrapper(key, peers_dht, system_dht, timeout)));

        let system_tker = Arc::clone(&self.system);
        let peers_tker = Arc::clone(&self.peers);
        let key = self.target_key.clone();
        futures.push(tokio::spawn(get_peers_from_tracker_wrapper(key, peers_tker, system_tker, timeout)));

        let peers_multicast = Arc::clone(&self.peers);
        let key = self.target_key.clone();
        futures.push(tokio::spawn(get_peers_from_multicast_wrapper(key, peers_multicast, timeout)));

        while let Some(res) = futures.next().await {
            if let Err(e) = res {
                warn!("Failed to get peers: {e}. ");
            }
        }
    }
}

async fn get_peers_from_tracker_wrapper(
    key: Key,
    peers: Arc<Mutex<HashSet<PeerInformation>>>,
    system: Arc<System>,
    timeout_ms: u64,
) -> Result<()> {
    timeout(Duration::from_millis(timeout_ms), get_peers_from_tracker(&key, peers, system)).await?
}

async fn get_peers_from_tracker(
    key: &Key,
    peers: Arc<Mutex<HashSet<PeerInformation>>>,
    system: Arc<System>,
) -> Result<()> {
    let trackers = {
        let (tx, rx) = oneshot::channel();
        system.tracker_list_command_tx.send(TrackerListCommand::Get(tx))?;
        let trackers = rx.await?;
        trackers.into_iter().collect::<Vec<Node>>()
    };

    let new_peers = announce_to_trackers(&trackers, key, true, system).await;
    let new_peers: Vec<_> = new_peers.into_iter().map(|p| PeerInformation::from(p)).collect();
    debug!("Peer aggregator got {} peers from tracker(s). ", new_peers.len());

    let mut peers = peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
    for peer in new_peers {
        peers.insert(peer);
    }

    Ok(())
}

async fn get_peers_from_dht_wrapper(
    key: Key,
    peers: Arc<Mutex<HashSet<PeerInformation>>>,
    system: Arc<System>,
    timeout_ms: u64,
) -> Result<()> {
    timeout(Duration::from_millis(timeout_ms), get_peers_from_dht(&key, peers, system)).await?
}

async fn get_peers_from_dht(
    key: &Key,
    peers: Arc<Mutex<HashSet<PeerInformation>>>,
    system: Arc<System>,
) -> Result<()> {
    let new_peers = announce(key, system).await?;
    debug!("Peer aggregator got {} peers from DHT. ", new_peers.len());

    let new_peers: HashSet<_> = new_peers
        .into_iter()
        .map(|p| PeerInformation::from(p))
        .collect();

    let mut peers = peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
    for peer in new_peers {
        peers.insert(peer);
    }

    Ok(())
}

async fn get_peers_from_multicast_wrapper(
    key: Key,
    peers: Arc<Mutex<HashSet<PeerInformation>>>,
    timeout_ms: u64,
) -> Result<()> {
    timeout(Duration::from_millis(timeout_ms), get_peers_from_multicast(&key, peers)).await?
}

async fn get_peers_from_multicast(
    key: &Key,
    peers: Arc<Mutex<HashSet<PeerInformation>>>,
) -> Result<()> {
    let new_peers = find_peer(key).await;
    debug!("Peer aggregator got {} peers from multicast. ", new_peers.len());

    let new_peers: HashSet<_> = new_peers
        .into_iter()
        .map(|p| PeerInformation::from(p))
        .collect();

    let mut peers = peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
    for peer in new_peers {
        peers.insert(peer);
    }

    Ok(())
}
