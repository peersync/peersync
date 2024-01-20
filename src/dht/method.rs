use std::{
    collections::HashSet,
    sync::Arc,
};

use anyhow::Result;
use itertools::Itertools;
use log::{debug, warn};
use tokio::sync::oneshot;

use crate::{
    dht::{
        protocol::NodeAndDistance,
        KBucketCommand,
    },
    node::{Distance, Key, Node},
    system::System,
};

pub async fn ping(target_node: &Node, system: Arc<System>) -> Result<Node> {
    Ok(target_node.send_ping_to_this(system).await?)
}

pub async fn find_node(
    key: &Key,
    system: Arc<System>,
) -> Result<Vec<NodeAndDistance>> {
    let (tx, rx) = oneshot::channel();
    system.kb_command_tx.send(KBucketCommand::DumpNodes(tx))?;
    let init_peers = rx.await?;
    let init_peers: Vec<_> = init_peers
        .into_iter()
        .map(|p| {
            let distance = Distance::new(&p.key, &key);
            NodeAndDistance(p, distance)
        })
        .collect();

    let key = Arc::new(key.clone());

    let (tx, rx) = oneshot::channel();
    let command_key = (*key).clone();
    let command = KBucketCommand::FindClosestNodes((command_key, tx));
    system.kb_command_tx.send(command)?;

    let mut queried: HashSet<_> = rx.await?.into_iter().collect();
    queried.extend(init_peers);
    let mut targets_to_query = queried.clone();

    let peers = loop {
        debug!("Executing FindNode on {} node(s). ", targets_to_query.len());

        let results: Vec<_> = targets_to_query.into_iter().map(|target| {
            let key = Arc::clone(&key);
            let system = Arc::clone(&system);

            tokio::spawn(async move {
                target.0.send_find_node_to_this(&key, system).await.unwrap_or_else(|e| {
                    warn!("Failed to call FindNode on remote node {}: {e}. ", target.0.name);
                    vec![]
                })
            })
        }).collect();

        let mut response = HashSet::new();
        for result in results {
            match result.await {
                Ok(nad) => response.extend(nad.into_iter()),
                Err(e) => {
                    warn!("FindNode failed: {e}. ");
                    continue;
                },
            };
        }

        if response.len() >= 3 {
            // enough peers
            let result: HashSet<_> = response.into_iter().unique().collect();
            debug!("Got {} peer(s) from DHT. ", result.len());
            break result;
        }

        let diff: HashSet<_> = response.difference(&queried).map(|x| x.clone()).collect();
        if diff.len() == 0 {
            let result: HashSet<_> = queried.into_iter().unique().collect();
            debug!("Got {} peer(s) from DHT. ", result.len());
            break result;
        }

        queried = queried.union(&queried).map(|x| x.clone()).collect();
        targets_to_query = diff;
    };

    let peers: Vec<_> = peers.into_iter().unique().collect();
    Ok(peers)
}

pub async fn announce(
    key: &Key,
    system: Arc<System>,
) -> Result<Vec<Node>> {
    let peers_to_announce = find_node(key, Arc::clone(&system)).await?;
    let key = Arc::new(key.clone());

    let results: Vec<_> = peers_to_announce.into_iter().map(|peer| {
        let system = Arc::clone(&system);
        let key = Arc::clone(&key);
        tokio::spawn(async move {
            peer.0.send_announce_to_this(&key, system).await
        })
    }).collect();

    let mut peers = vec![];
    for handle in results {
        match handle.await? {
            Ok(res) => peers.extend(res),
            Err(e) => {
                warn!("Announce failed: {e}. ");
                continue;
            }
        }
    }

    peers.retain(|node| node.key != system.local_node.key);

    let peers: Vec<_> = peers.into_iter().unique().collect();

    Ok(peers)
}
