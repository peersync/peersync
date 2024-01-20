use std::{
    fmt::{Display, Formatter},
    net::SocketAddr,
    result,
    sync::Arc,
};

use anyhow::{anyhow, Result};
use chrono::Utc;
use log::{debug, warn};
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};

use crate::{
    config::Bootstrap,
    dht::{method::{find_node, ping}, KBucketCommand},
    system::System,
    util::*,
};

pub const KEY_LENGTH: usize = 32;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Key ([u8; KEY_LENGTH]);

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Distance (pub [u8; KEY_LENGTH]);

#[derive(Serialize, Deserialize, Clone, Hash, Debug)]
pub struct Node {
    pub name: String,
    pub key: Key,
    pub addr: Vec<SocketAddr>,
    pub tls: bool,
    pub started: i64,
}

impl Node {
    pub fn new(name: String, key: Key, addr: Vec<SocketAddr>, tls: bool) -> Self {
        Node {
            name,
            key,
            addr,
            tls,
            started: Utc::now().timestamp(),
        }
    }

    pub fn uptime(&self) -> i64 {
        Utc::now().timestamp() - self.started
    }

    pub async fn bootstrap(
        &self,
        nodes: &Vec<Bootstrap>,
        system: Arc<System>,
    ) -> Result<()> {
        for bootstrap_node in nodes {
            let addr = match string_to_socket(&bootstrap_node.addr) {
                Ok(addr) => addr,
                Err(e) => {
                    warn!("Failed to parse bootstrap node address {}: {e}. ", bootstrap_node.addr);
                    continue;
                },
            };
            let dummy_node = Self::new(
                "dummy".into(),
                Key::default(),
                vec![addr],
                bootstrap_node.tls,
            );

            let response = match ping(&dummy_node, Arc::clone(&system)).await {
                Ok(node) => node,
                Err(e) => {
                    warn!("Failed to connect to bootstrap node {}: {e}. ", bootstrap_node.node_name);
                    continue;
                },
            };

            let command = KBucketCommand::AddNode(response);
            system.kb_command_tx.send(command)?;
        }
    
        debug!("Finding more peers via bootstrap peers. ");
        let kb_command_tx = system.kb_command_tx.clone();
        let close_nodes = find_node(&self.key, Arc::clone(&system)).await?;
        for node in close_nodes {
            if node.0.key == self.key {
                continue;
            }

            let command = KBucketCommand::AddNode(node.0);
            kb_command_tx.send(command)?;
        }
    
        Ok(())
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Node {}

impl Key {
    pub fn new(data: &[u8]) -> Self {
        let mut context = Context::new(&SHA256);
        context.update(data);
        let digest = context.finish();
        Self(digest.as_ref().try_into().unwrap())
    }

    pub fn from_str(string: &str) -> Result<Self> {
        if string.len() != 64 {
            return Err(anyhow!("invalid key: {string}"));
        }

        let parsed: Vec<_> = (0..string.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&string[i..i + 2], 16))
            .collect();

        let mut res = [0u8; KEY_LENGTH];

        for (i, v) in parsed.iter().enumerate() {
            res[i] = match v {
                Ok(v) => *v,
                Err(_) => return Err(anyhow!("invalid key: {string}")),
            };
        }

        Ok(Self(res))
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter) -> result::Result<(), std::fmt::Error> {
        for x in &self.0 {
            write!(f, "{:02X?}", x)?;
        }
        Ok(())
    }
}

impl Default for Key {
    fn default() -> Self {
        Self::new(&[0u8; KEY_LENGTH])
    }
}

impl Distance {
    pub fn new(x: &Key, y: &Key) -> Self {
        let mut res = [0u8; KEY_LENGTH];

        for i in 0..KEY_LENGTH {
            res[i] = x.0[i] ^ y.0[i];
        }

        Self(res)
    }
}
