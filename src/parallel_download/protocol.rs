use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::{
    extract::{Extension, Json},
    http::StatusCode,
    response::IntoResponse,
};
use reqwest::header;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
    node::{Key, Node},
    parallel_download::{local_storage::Range, LSCommand},
    system::System,
    util::*,
};

#[derive(Deserialize, Serialize)]
pub struct BlockStatusRequest {
    pub hash: Key,
}

#[derive(Deserialize, Serialize)]
pub struct BlockStatusResponse {
    pub ranges: Vec<Range>,
    pub available_layers: Vec<Key>,
}

pub async fn block_status_handler(
    Extension(system): Extension<Arc<System>>,
    Json(message): Json<BlockStatusRequest>,
) -> impl IntoResponse {
    match block_status_impl(message, system).await {
        Ok(response) => (StatusCode::OK, response).into_response(),
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
        }
    }
}

async fn block_status_impl(message: BlockStatusRequest, system: Arc<System>) -> Result<String> {
    let (tx, rx) = oneshot::channel();
    system.local_storage_command_tx.send(LSCommand::GetReady(message.hash, tx))?;
    let ranges = rx.await?;

    let (tx, rx) = oneshot::channel();
    system.local_storage_command_tx.send(LSCommand::GetAllReady(tx))?;
    let available_layers = rx.await?;

    let response = BlockStatusResponse {
        ranges,
        available_layers,
    };

    let response = serde_json::to_string(&response)?;
    Ok(response)
}

pub async fn get_peer_block_status(target: &Node, hash: Key, system: Arc<System>) -> Result<(Vec<Range>, Vec<Key>)> {
    let addr = addr_from_node(&target).ok_or(anyhow!("peer {} has no address", target.name))?;
    let uri = format!("{}://{addr}/peer/block_status", proto_from_node(&target));

    let request = BlockStatusRequest {
        hash,
    };

    let client = system.client.clone();
    let response = client
        .get(uri)
        .json(&request)
        .header(header::HOST, &target.name)
        .send()
        .await?;

    let response: BlockStatusResponse = response.json().await?;

    Ok((response.ranges, response.available_layers))
}
