use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Extension, Json},
    http::{header, StatusCode},
    response::IntoResponse,
};
use log::debug;
use tokio::sync::oneshot;

use crate::{
    dht::{protocol::{IncomingRequest, Response}, KBucketCommand},
    node::{Key, Node},
    parallel_download::LSCommand,
    peer::PeerCommand,
    system::System,
};

pub async fn dht_handler(
    Extension(system): Extension<Arc<System>>,
    Json(request): Json<IncomingRequest>,
) -> impl IntoResponse {
    let response = match dht_handler_impl(request, system).await {
        Ok(response) => response,
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        },
    };

    (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], response).into_response()
}

async fn dht_handler_impl(
    request: IncomingRequest,
    system: Arc<System>,
) -> Result<String> {
    match request {
        IncomingRequest::Ping(node) => ping_handler(node, system).await,
        IncomingRequest::Announce(node, hash) => announce_handler(hash, node, system).await,
        IncomingRequest::FindNode(target_key) => find_node_handler(target_key, system).await,
    }
}

async fn ping_handler(
    remote_node: Node,
    system: Arc<System>,
) -> Result<String> {
    debug!("Received Ping request from {}. ", remote_node.name);

    let command = KBucketCommand::AddNode(remote_node);
    system.kb_command_tx.send(command)?;

    let response = Response::Ping(&system.local_node);
    let response = serde_json::to_string(&response)?;
    Ok(response)
}

async fn find_node_handler(
    target_key: Key,
    system: Arc<System>,
) -> Result<String> {
    debug!("Received FindNode request for {}. ", target_key);

    let (tx, rx) = oneshot::channel();
    let command = KBucketCommand::FindClosestNodes((target_key, tx));
    system.kb_command_tx.send(command)?;

    let res = rx.await?;
    debug!("Returning {} node(s). ", res.len());

    let response = Response::FindNode(&res);
    let response = serde_json::to_string(&response)?;
    Ok(response)
}

async fn announce_handler(
    hash: Key,
    node: Node,
    system: Arc<System>,
) -> Result<String> {
    debug!("Received Announce request for {hash} from {}. ", node.name);

    let command = KBucketCommand::AddNode(node.clone());
    system.kb_command_tx.send(command)?;

    let (tx, rx) = oneshot::channel();
    let command = PeerCommand::GetPeers((hash.clone(), tx));
    system.peer_command_tx.send(command)?;

    // don't forget myself
    let has_layer = {
        let (tx, rx) = oneshot::channel();
        system.local_storage_command_tx.send(LSCommand::GetReady(hash.clone(), tx))?;
        !rx.await?.is_empty()
    };

    let mut res = rx.await?;

    if has_layer {
        res.insert(system.local_node.clone());
    }

    debug!("Returning {} node(s). ", res.len());

    let command = PeerCommand::AddPeer((hash, node));
    system.peer_command_tx.send(command)?;

    let response = Response::Announce(&res);
    let response = serde_json::to_string(&response)?;
    Ok(response)
}
