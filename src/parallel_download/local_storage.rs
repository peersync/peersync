use std::{cmp::max, collections::{HashMap, HashSet, VecDeque}};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot::Sender};

use crate::{docker::ImageManifest, node::Key};

const AVG_SLIDING_WINDOW_PER_PEER: usize = 10;
const AVG_SLIDING_WINDOW_GLOBAL: usize = 100;
const LAMBDA: f64 = 0.5;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Range(pub usize, pub usize);

pub struct LocalStorage;

struct PeerSpeed {
    recent_speeds: VecDeque<f64>,
}

struct PeerSpeedManager {
    speed: HashMap<Key, PeerSpeed>,
    global_recent_speeds: VecDeque<f64>,
}

impl Default for PeerSpeedManager {
    fn default() -> Self {
        Self {
            speed: HashMap::new(),
            global_recent_speeds: VecDeque::new(),
        }
    }
}

pub enum Command {
    AddReady(Key, Range),
    GetReady(Key, Sender<Vec<Range>>),
    RemoveReady(Key),
    GetAllReady(Sender<Vec<Key>>),
    SetSize(Key, usize),
    GetSize(Key, Sender<Option<usize>>),
    AddManifest(String, ImageManifest),
    GetManifest(String, Sender<Option<ImageManifest>>),
    AddLocalNode(Key),
    IsLocalNode(Key, Sender<bool>),
    AddSpeedDataPoint(Key, f64),
    GetAvgSpeedDiff(Key, Sender<f64>), // calculates the speed diff of a given peer and global peers
}

impl Range {
    pub fn has_block(&self, block: usize) -> bool {
        block >= self.0 && block <= self.1
    }
}

impl LocalStorage {
    pub async fn start(mut rx: UnboundedReceiver<Command>) -> Result<()> {
        let mut ready_storage = HashMap::new();
        let mut ms = HashMap::new();
        let mut ss = HashMap::new();
        let mut local_nodes = HashSet::new();
        let mut speed_manager = PeerSpeedManager::default();

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::AddReady(key, range) => add_ready_impl(&mut ready_storage, key, range)?,
                Command::GetReady(key, tx) => get_ready_impl(&ready_storage, key, tx)?,
                Command::RemoveReady(key) => remove_ready_impl(&mut ready_storage, key)?,
                Command::GetAllReady(tx) => get_all_ready_impl(&ready_storage, tx)?,
                Command::SetSize(key, size) => set_size_impl(&mut ss, key, size)?,
                Command::GetSize(key, tx) => get_size_impl(&ss, key, tx)?,
                Command::AddManifest(name, mn) => add_manifest_impl(&mut ms, name, mn)?,
                Command::GetManifest(name, tx) => get_manifest_impl(&ms, name, tx)?,
                Command::AddLocalNode(key) => add_local_node_impl(&mut local_nodes, key)?,
                Command::IsLocalNode(key, tx) => is_local_node_impl(&local_nodes, key, tx)?,
                Command::AddSpeedDataPoint(key, speed) => add_speed_data_point_impl(&mut speed_manager, key, speed)?,
                Command::GetAvgSpeedDiff(key, tx) => get_avg_speed_diff(&speed_manager, key, tx)?,
            }
        }

        Ok(())
    }
}

fn set_size_impl(storage: &mut HashMap<Key, usize>, key: Key, size: usize) -> Result<()> {
    storage.insert(key, size);
    Ok(())
}

fn get_size_impl(
    storage: &HashMap<Key, usize>,
    key: Key,
    tx: Sender<Option<usize>>,
) -> Result<()> {
    let size = storage.get(&key).cloned();
    tx.send(size).map_err(|_| anyhow!("failed to acquire lock"))?;
    Ok(())
}

fn add_ready_impl(storage: &mut HashMap<Key, Vec<Range>>, key: Key, range: Range) -> Result<()> {
    let ranges = storage.entry(key.clone()).or_insert(vec![]);

    ranges.push(range);

    let mut end = 0;
    ranges.iter().for_each(|r| end = max(end, r.1));

    let mut axis = vec![false; end + 1];
    ranges
        .iter()
        .for_each(|r| (r.0..=r.1).into_iter().for_each(|i| axis[i] = true));

    let mut res = vec![];
    let mut open = false;
    let mut left = 0;

    for i in 0..axis.len() {
        if !open && axis[i] {
            open = true;
            left = i;
        } else if open && axis[i] {
            // do nothing
        } else if !open && !axis[i] {
            // do nothing
        } else {
            res.push(Range(left, i - 1));
            open = false;
        }
    }

    if open {
        res.push(Range(left, axis.len()));
    }

    *ranges = res;

    Ok(())
}

fn remove_ready_impl(storage: &mut HashMap<Key, Vec<Range>>, key: Key) -> Result<()> {
    storage.remove(&key);
    Ok(())
}

fn get_ready_impl(
    storage: &HashMap<Key, Vec<Range>>,
    key: Key,
    tx: Sender<Vec<Range>>,
) -> Result<()> {
    let ranges = storage.get(&key).cloned().unwrap_or(vec![]);
    tx.send(ranges).map_err(|_| anyhow!("failed to acquire lock"))?;
    Ok(())
}

fn get_all_ready_impl(
    storage: &HashMap<Key, Vec<Range>>,
    tx: Sender<Vec<Key>>,
) -> Result<()> {
    let keys: Vec<_> = storage.iter().map(|(key, _)| key.clone()).collect();
    tx.send(keys).map_err(|_| anyhow!("failed to acquire lock"))?;
    Ok(())
}

fn add_manifest_impl(
    storage: &mut HashMap<String, ImageManifest>,
    name: String,
    manifest: ImageManifest,
) -> Result<()> {
    storage.insert(name, manifest);
    Ok(())
}

fn get_manifest_impl(
    storage: &HashMap<String, ImageManifest>,
    name: String,
    tx: Sender<Option<ImageManifest>>,
) -> Result<()> {
    let manifest = storage.get(&name).cloned();
    tx.send(manifest).map_err(|_| anyhow!("failed to acquire lock"))?;
    Ok(())
}

fn add_local_node_impl(storage: &mut HashSet<Key>, key: Key) -> Result<()> {
    storage.insert(key);
    Ok(())
}

fn is_local_node_impl(storage: &HashSet<Key>, key: Key, tx: Sender<bool>) -> Result<()> {
    let is_local = storage.contains(&key);
    tx.send(is_local).map_err(|_| anyhow!("failed to acquire lock"))?;
    Ok(())
}

fn add_speed_data_point_impl(storage: &mut PeerSpeedManager, key: Key, speed: f64) -> Result<()> {
    // per peer
    let peer = storage.speed.entry(key.clone()).or_insert(PeerSpeed {
        recent_speeds: VecDeque::new(),
    });

    peer.recent_speeds.push_back(speed);
    if peer.recent_speeds.len() > AVG_SLIDING_WINDOW_PER_PEER {
        peer.recent_speeds.pop_front();
    }

    // global
    storage.global_recent_speeds.push_back(speed);

    if storage.global_recent_speeds.len() > AVG_SLIDING_WINDOW_GLOBAL {
        storage.global_recent_speeds.pop_front();
    }

    Ok(())
}

fn get_avg_speed_diff(storage: &PeerSpeedManager, key: Key, tx: Sender<f64>) -> Result<()> {
    let weights: Vec<_> = (0..storage.global_recent_speeds.len())
        .map(|i| (-1.0 * i as f64 * LAMBDA).exp())
        .collect();
    let weight_sum: f64 = weights.iter().sum();
    let value_sum: f64 = storage
        .global_recent_speeds
        .iter()
        .zip(weights)
        .map(|(s, w)| s * w)
        .sum();
    let mut global_avg = value_sum / weight_sum;

    if !global_avg.is_normal() {
        global_avg = 0.0;
    }

    let mut peer_avg: f64 = match storage.speed.get(&key) {
        Some(peer) => {
            let weights: Vec<_> = (0..peer.recent_speeds.len())
                .map(|i| (-1.0 * i as f64 * LAMBDA).exp())
                .collect();
            let weight_sum: f64 = weights.iter().sum();
            let value_sum: f64 = peer.recent_speeds.iter().zip(weights).map(|(s, w)| s * w).sum();
            value_sum / weight_sum
        },
        None => 0.0, // if the peer is not included
    };

    if !peer_avg.is_normal() {
        peer_avg = 0.0;
    }

    let diff = peer_avg - global_avg;

    tx.send(diff).map_err(|_| anyhow!("failed to acquire lock"))?;

    Ok(())
}
