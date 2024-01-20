use std::{collections::{HashMap, HashSet}, sync::Mutex};

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot::Sender};

use crate::node::{Node, Key};

pub enum PeerCommand {
    GetPeers((Key, Sender<HashSet<Node>>)),
    AddPeer((Key, Node)),
}

pub struct PeerManager {
    peers: Mutex<HashMap<Key, HashSet<Node>>>,
    command_rx: UnboundedReceiver<PeerCommand>,
}

impl PeerManager {
    pub fn new(command_rx: UnboundedReceiver<PeerCommand>) -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
            command_rx,
        }
    }

    pub async fn start_manager(mut self) -> Result<()> {
        loop {
            let command = self.command_rx.recv().await.ok_or(anyhow!("failed to receive command"))?;
            self.command_handler(command)?;
        }
    }

    fn command_handler(&self, command: PeerCommand) -> Result<()> {
        match command {
            PeerCommand::GetPeers((key, tx)) => self.get_peers(key, tx)?,
            PeerCommand::AddPeer((key, node)) => self.add_peer(key, node)?,
        }

        Ok(())
    }

    fn get_peers(&self, key: Key, tx: Sender<HashSet<Node>>) -> Result<()> {
        let peers = self.peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.get(&key).cloned();
        let peers = peers.unwrap_or(HashSet::new());
        tx.send(peers).map_err(|_| anyhow!("failed to send response"))?;
        Ok(())
    }

    fn add_peer(&self, key: Key, node: Node) -> Result<()> {
        let mut peers = self.peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
        let peers = peers.entry(key).or_insert(HashSet::new());

        if peers.contains(&node) {
            // Updated to latest peer information
            peers.remove(&node);
        }

        peers.replace(node);

        Ok(())
    }
}
