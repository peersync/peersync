use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::{
    extract::{Extension, Json},
    http::{header, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::{
    merkle_tree::r#impl::AdditionalHashes,
    node::{Key, Node},
    system::System,
    util::*,
};

#[derive(Deserialize, Serialize)]
pub struct MerkleTreeRequest {
    key: Key,
}

#[derive(Deserialize, Serialize)]
struct MerkleTreeResponse {
    hashes: AdditionalHashes,
}

pub async fn request_hashes(key: &Key, target: &Node, system: Arc<System>) -> Result<AdditionalHashes> {
    let addr = addr_from_node(&target).ok_or(anyhow!("peer {} has no address", target.name))?;
    let uri = format!("{}://{addr}/mt/req", proto_from_node(&target));

    let request = MerkleTreeRequest {
        key: key.clone(),
    };

    let response = system
        .client
        .get(uri)
        .json(&request)
        .header(header::HOST, &target.name)
        .header(header::CONTENT_TYPE, "application/json")
        .send()
        .await?;

    let response = response.bytes().await?;
    let response: MerkleTreeResponse = serde_json::from_slice(&response)?;

    Ok(response.hashes)
}

pub async fn req_handler(
    Extension(system): Extension<Arc<System>>,
    Json(request): Json<MerkleTreeRequest>,
) -> impl IntoResponse {
    let response = match req_impl(request, system) {
        Ok(res) => res,
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        },
    };

    let response = match response {
        Some(response) => response,
        None => return StatusCode::NOT_FOUND.into_response(),
    };

    (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], response).into_response()
}

fn req_impl(request: MerkleTreeRequest, system: Arc<System>) -> Result<Option<String>> {
    let resp = match system.merkle_tree_storage.dump(&request.key)? {
        Some(resp) => resp,
        None => return Ok(None),
    };

    let resp = MerkleTreeResponse {
        hashes: resp,
    };

    let resp = serde_json::to_string(&resp)?;
    
    Ok(Some(resp))
}
