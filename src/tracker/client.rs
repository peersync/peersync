use std::{collections::HashSet, sync::Arc};

use anyhow::{anyhow, Result};
use itertools::Itertools;
use log::{debug, warn};
use reqwest::{header, Client};

use crate::{
    node::{Key, Node},
    system::System,
    tracker::protocol::*,
    util::*,
};

pub async fn announce_to_trackers(
    trackers: &Vec<Node>,
    key: &Key,
    announce: bool,
    system: Arc<System>,
) -> Vec<Node> {
    let key = Arc::new(key.clone());

    let exec = |tracker: &Node| {
        let tracker = tracker.clone();
        let key = Arc::clone(&key);
        let system = Arc::clone(&system);

        tokio::spawn(async move {
            let res = announce_to_tracker_impl(&tracker, key, announce, system).await;
            res.unwrap_or_else(|e| {
                warn!("Failed to announce to tracker {}: {e}. ", tracker.name);
                HashSet::new()
            })
        })
    };

    let handles: Vec<_> = trackers.iter().map(exec).collect();

    let mut peers = vec![];
    for handle in handles {
        let result = match handle.await {
            Ok(result) => result,
            Err(e) => {
                warn!("Failed to complete announce: {e}. ");
                continue;
            }
        };

        peers.extend(result);
    }

    let peers: Vec<_> = peers.into_iter().unique().collect();

    peers
}

async fn announce_to_tracker_impl(
    tracker: &Node,
    key: Arc<Key>,
    announce: bool,
    system: Arc<System>,
) -> Result<HashSet<Node>> {
    let node = if announce {
        Some(&system.local_node)
    } else {
        None
    };

    let request = GetRequest {
        key: &*key,
        node,
    };

    let addr = addr_from_node(&tracker).ok_or(anyhow!("peer {} has no address", tracker.name))?;
    let uri = format!("{}://{addr}/tracker/get", proto_from_node(&tracker));

    let response = system
        .client
        .get(uri)
        .json(&request)
        .header(header::HOST, &tracker.name)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await?;

    let response: GetResponse = response.json().await?;

    Ok(response.peers)
}

pub async fn get_trackers_from_node(client: Client, target: &Node) -> Result<HashSet<Node>> {
    let addr = addr_from_node(&target).ok_or(anyhow!("peer {} has no address", target.name))?;
    let uri = format!("{}://{addr}/tracker/get_trackers", proto_from_node(&target));

    let response = client
        .get(uri)
        .header(header::HOST, &target.name)
        .send()
        .await?;

    let response: GetTrackersResponse = response.json().await?;

    Ok(response.trackers)
}

pub async fn ping_tracker(client: Client, target: Node) -> bool {
    ping_tracker_impl(client, &target).await.unwrap_or(false)
}

async fn ping_tracker_impl(client: Client, target: &Node) -> Result<bool> {
    let addr = addr_from_node(&target).ok_or(anyhow!("peer {} has no address", target.name))?;
    let uri = format!("{}://{addr}/tracker/get_trackers", proto_from_node(&target));

    let response = client
        .get(uri)
        .header(header::HOST, &target.name)
        .send()
        .await?;

    Ok(response.status().as_u16() == 200)
}

pub async fn request_election(
    client: Client,
    message: ElectionMessage,
    target: &Node,
) -> Result<()> {
    debug!("Sending tracker election request to {}. ", target.name);

    let addr = addr_from_node(&target).ok_or(anyhow!("peer {} has no address", target.name))?;
    let uri = format!(
        "{}://{addr}/tracker/start_election",
        proto_from_node(&target)
    );

    let response = client
        .get(uri)
        .json(&message)
        .header(header::HOST, &target.name)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await?;

    if response.status().as_u16() != 200 {
        let text = response.text().await?;
        return Err(anyhow!("failed to request election: {text}"));
    }

    Ok(())
}

pub async fn announce_election_result(
    client: Client,
    tracker: Arc<Node>,
    target: Node,
) -> Result<()> {
    debug!("Sending tracker election result to {}. ", target.name);

    let addr = addr_from_node(&target).ok_or(anyhow!("peer {} has no address", target.name))?;
    let uri = format!(
        "{}://{addr}/tracker/election_result",
        proto_from_node(&target)
    );

    let response = client
        .get(uri)
        .json(&*tracker)
        .header(header::HOST, &target.name)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await?;

    if response.status().as_u16() != 200 {
        let text = response.text().await?;
        return Err(anyhow!("failed to request election: {text}"));
    }

    Ok(())
}
