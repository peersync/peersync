use std::{cmp::min, sync::{Arc, Mutex}};

use anyhow::{anyhow, Error, Result};
use log::{debug, info, warn};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot::Sender};

use crate::{
    dht::{method::ping, protocol::NodeAndDistance},
    node::{KEY_LENGTH, Distance, Key, Node},
    system::System,
};

pub enum KBucketCommand {
    AddNode(Node),
    FindClosestNodes((Key, Sender<Vec<NodeAndDistance>>)),
    DumpNodes(Sender<Vec<Node>>),
    DeleteNode(Key),
}

pub struct KBucket {
    nodes: Arc<Mutex<Vec<Vec<Node>>>>,
    k_param: usize,
    command_rx: UnboundedReceiver<KBucketCommand>,
    system: Arc<System>,
}

impl KBucket {
    pub fn new(
        k_param: usize,
        command_rx: UnboundedReceiver<KBucketCommand>,
        system: Arc<System>,
    ) -> Self {
        Self {
            nodes: Arc::new(Mutex::new(vec![vec![]; KEY_LENGTH * 8])),
            k_param,
            command_rx,
            system,
        }
    }

    pub async fn start_manager(mut self) -> Result<()> {
        loop {
            let command = self.command_rx.recv().await.ok_or(anyhow!("failed to receive command"))?;
            self.command_handler(command)?;
        }
    }

    fn command_handler(&self, command: KBucketCommand) -> Result<()> {
        match command {
            KBucketCommand::AddNode(node) => self.add_node(Arc::clone(&self.system), node)?,
            KBucketCommand::FindClosestNodes((key, tx)) => self.find_node(key, tx)?,
            KBucketCommand::DumpNodes(tx) => self.get_all_nodes(tx)?,
            KBucketCommand::DeleteNode(key) => self.delete_node(key)?,
        }

        Ok(())
    }

    fn get_all_nodes(&self, tx: Sender<Vec<Node>>) -> Result<()> {
        let kb = self.nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?.clone();
        let mut response = vec![];
        kb.into_iter().for_each(|bucket| response.extend(bucket));
        tx.send(response).map_err(|_| anyhow!("failed to send response"))?;
        Ok(())
    }

    fn delete_node(&self, key: Key) -> Result<()> {
        let bucket = self.get_bucket_index_by_key(&key);
        self.nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?[bucket].retain(|n| n.key != key);
        Ok(())
    }

    fn find_node(&self, key: Key, tx: Sender<Vec<NodeAndDistance>>) -> Result<()> {
        let k_param = self.k_param;

        if k_param == 0 {
            tx.send(vec![]).map_err(|_| anyhow!("failed to send response"))?;
            return Ok(());
        }

        let mut res = {
            let mut res = vec![];
            let base = self.get_bucket_index_by_key(&key);

            let kb = self.nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
    
            for node in &kb[base] {
                res.push(NodeAndDistance(node.clone(), Distance::new(&key, &node.key)));
            }
    
            let (mut front, mut rear) = (base as isize, base);
            while res.len() < k_param as usize {
                front -= 1;
                rear += 1;
    
                if front >= 0 {
                    for node in &kb[front as usize] {
                        res.push(NodeAndDistance(node.clone(), Distance::new(&key, &node.key)));
                    }
                }
    
                if rear < kb.len() {
                    for node in &kb[rear] {
                        res.push(NodeAndDistance(node.clone(), Distance::new(&key, &node.key)));
                    }
                }
    
                if front < 0 && rear >= kb.len() {
                    break;
                }
            }

            res
        };
    
        res.sort_by(|a, b| a.1.cmp(&b.1));
        res.truncate(min(k_param as usize, res.len()));

        tx.send(res).map_err(|_| anyhow!("failed to send response"))?;

        Ok(())
    }

    fn add_node(&self, system: Arc<System>, node: Node) -> Result<()> {
        let index = self.get_bucket_index_by_key(&node.key);
        let nodes = Arc::clone(&self.nodes);
        let k_param = self.k_param;
        let system = Arc::clone(&system);

        tokio::spawn(async move {
            debug!("Adding or updating node {}. ", node.name);

            let bucket_size = nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?[index].len();

            if delete_node_impl(&nodes, index, &node.key)? { // If the node already exists, update it
                debug!("Updating node {} in bucket {}. ", node.name, index);
                nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?[index].push(node);
            } else if bucket_size < k_param as usize { // If the corresponding k-bucket is not full, insert it
                debug!("Adding node {} to bucket {}. ", node.name, index);
                nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?[index].push(node);
            } else { // If the corresponding k-bucket is full, ping the oldest node
                let pending = nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?[index][0].clone();
        
                let new_node = match ping(&pending, system).await {
                    Ok(renewed) => {
                        debug!("Updating node {} in bucket {}. ", renewed.name, index);
                        debug!("Dropping node {}. ", node.name);
                        renewed
                    },
                    Err(e) => {
                        warn!("Failed to ping node {}: {e}. ", pending.name);
                        debug!("Adding node {} to bucket {}. ", node.name, index);
                        debug!("Dropping node {}. ", pending.name);
                        node
                    },
                };
        
                if delete_node_impl(&nodes, index, &pending.key)? {
                    nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?[index].push(new_node);
                } else {
                    warn!("Attempted to remove non-exist node. ");
                }
            }

            Ok::<(), Error>(())
        });

        Ok(())
    }

    fn get_bucket_index_by_key(&self, foreign_key: &Key) -> usize {
        let distance = Distance::new(&self.system.local_node.key, &foreign_key);

        for i in 0..KEY_LENGTH {
            for j in (0..8).rev() {
                if (distance.0[i] >> (7 - j)) & 0x1 != 0 {
                    return i * 8 + j;
                }
            }
        }

        KEY_LENGTH * 8 - 1
    }
}

fn delete_node_impl(nodes: &Arc<Mutex<Vec<Vec<Node>>>>, index: usize, key: &Key) -> Result<bool> {
    let mut nodes = nodes.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
    if let Some(i) = nodes[index].iter().position(|x| &x.key == key) {
        info!("Removing node {key} from peer table. ");
        nodes[index].remove(i);
    } else {
        return Ok(false);
    }

    Ok(true)
}
