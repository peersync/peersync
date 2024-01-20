use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use log::warn;
use reqwest::{header, StatusCode};

use crate::{
    dht::protocol::{NodeAndDistance, Request, IncomingResponse},
    node::{Node, Key},
    system::System,
};

impl Node {
    pub async fn send_ping_to_this(&self, system: Arc<System>) -> Result<Node> {
        let request = Request::Ping(&system.local_node);
        let system = Arc::clone(&system);
        let response = send_request_to_node(&request, self, system).await?;
        let response = match response {
            IncomingResponse::Ping(res) => res,
            _ => return Err(anyhow!("invalid response from {}", self.name)),
        };
        Ok(response) 
    }

    pub async fn send_find_node_to_this(&self, key: &Key, system: Arc<System>) -> Result<Vec<NodeAndDistance>> {
        let request = Request::FindNode(key);
        let response = send_request_to_node(&request, self, system).await?;
        let response = match response {
            IncomingResponse::FindNode(res) => res,
            _ => return Err(anyhow!("invalid response from {}", self.name)),
        };
        Ok(response)
    }

    pub async fn send_announce_to_this(&self, key: &Key, system: Arc<System>) -> Result<HashSet<Node>> {
        let request = Request::Announce(&system.local_node, key);
        let system = Arc::clone(&system);
        let response = send_request_to_node(&request, self, system).await?;
        let response = match response {
            IncomingResponse::Announce(res) => res,
            _ => return Err(anyhow!("invalid response from {}", self.name)),
        };
        Ok(response)
    }
}

async fn send_request_to_node(req: &Request<'_>, remote_node: &Node, system: Arc<System>) -> Result<IncomingResponse> {
    for remote_socket in &remote_node.addr {
        match send_request_to_addr(&req, remote_socket, &remote_node.name, remote_node.tls, &system).await {
            Ok(resp) => return Ok(resp),
            Err(e) => warn!("Failed to connect to node {} at {remote_socket}: {e}. ", remote_node.name),
        };
    }

    Err(anyhow!("failed to reach remote node {}", remote_node.name))
}

pub async fn send_request_to_addr(
    req: &Request<'_>,
    remote_addr: &SocketAddr,
    hostname: &str,
    tls: bool,
    system: &Arc<System>,
) -> Result<IncomingResponse> {
    let remote = if tls {
        format!("https://{remote_addr}/dht")
    } else {
        format!("http://{remote_addr}/dht")
    };

    let response = system.client
        .get(remote)
        .json(&req)
        .header(header::HOST, hostname)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await?;

    if response.status() != StatusCode::OK {
        let status = response.status().as_u16();
        return Err(anyhow!("remote {hostname} returned non-OK status code {status}"));
    }

    let response: IncomingResponse = response.json().await?;

    Ok(response)
}
