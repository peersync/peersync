use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{
    extract::{ConnectInfo, Extension, Path},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::IntoResponse,
};
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use url::Url;

use crate::{node::Key, parallel_download::LSCommand, system::System, util::*};

const MANIFEST_MIME: &str = "application/vnd.docker.distribution.manifest.v2+json";

#[derive(Clone, Deserialize, Serialize)]
struct ImageConfig {
    #[serde(rename = "mediaType", skip_serializing_if = "Option::is_none")]
    #[allow(unused)]
    media_type: Option<String>,

    #[allow(unused)]
    size: usize,

    #[allow(unused)]
    digest: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct ImageLayer {
    #[serde(rename = "mediaType", skip_serializing_if = "Option::is_none")]
    #[allow(unused)]
    media_type: Option<String>,

    size: usize,
    digest: String,

    #[allow(unused)]
    urls: Option<Vec<String>>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ImageManifest {
    #[serde(rename = "schemaVersion")]
    #[allow(unused)]
    schema_version: u64,

    #[serde(rename = "mediaType", skip_serializing_if = "Option::is_none")]
    #[allow(unused)]
    media_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[allow(unused)]
    config: Option<ImageConfig>,

    layers: Vec<ImageLayer>,
}

enum RequestMethod {
    Get,
}

pub async fn manifest_handler(
    Path(params): Path<HashMap<String, String>>,
    method: Method,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Extension(system): Extension<Arc<System>>,
) -> impl IntoResponse {
    match method {
        Method::GET => {},
        _ => {
            let message = format!("Requested method not implemented. ");
            return (StatusCode::BAD_REQUEST, message).into_response();
        },
    }

    let name = match params.get("name") {
        Some(name) => name,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    let reference = match params.get("reference") {
        Some(reference) => reference,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    let name = match params.get("repo") {
        Some(repo) => format!("{repo}/{name}"),
        None => name.to_owned(),
    };

    debug!("Got Docker registry manifest GET request for {name}:{reference} from {addr}. ");

    let (headers, resp) = match manifest_impl(name, reference, headers, system).await {
        Ok((headers, resp)) => (headers, resp),
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            return (StatusCode::BAD_REQUEST, message).into_response();
        }
    };

    (StatusCode::OK, headers, resp).into_response()
}

async fn manifest_impl(
    name: String,
    reference: &str,
    headers: HeaderMap,
    system: Arc<System>,
) -> Result<(HeaderMap, String)> {
    let cache_name = format!("{name}:{reference}");
    let (tx, rx) = oneshot::channel();
    system.local_storage_command_tx.send(LSCommand::GetManifest(cache_name.clone(), tx))?;
    if let Some(manifest) = rx.await? {
        debug!("Manifest {} found in cache. ", cache_name);
        let manifest = serde_json::to_string(&manifest)?;
        let mut headers = HeaderMap::new();
        headers.append(header::CONTENT_TYPE, HeaderValue::from_static(MANIFEST_MIME));
        return Ok((headers, manifest));
    }

    let req_path = format!("/v2/{name}/manifests/{reference}");
    let system_req = Arc::clone(&system);
    let (headers, status_code, body) =
        relay_request(&req_path, RequestMethod::Get, headers, system_req).await?;

    if status_code.as_u16() != 200 {
        let body = std::str::from_utf8(&body)?;
        return Err(anyhow!(
            "non-OK response from remote registry {status_code}: {body}"
        ));
    }

    let manifest: ImageManifest = serde_json::from_slice(&body)?;

    debug!("Received manifest with {} layers. ", manifest.layers.len());

    for layer in &manifest.layers {
        debug!("Layer: {}. ", layer.digest);

        let hash = if layer.digest.starts_with("sha256:") {
            Key::from_str(&layer.digest[7..])?
        } else {
            return Err(anyhow!("invalid hash {}", layer.digest));
        };

        system.local_storage_command_tx.send(LSCommand::SetSize(hash.clone(), layer.size))?;

        // if byte 0 ~ (4MiB - 1): we have block 0 in full
        // len = 4MiB at that time
        let blocks = byte_to_block(0, layer.size - 1, layer.size);
        system.local_storage_command_tx.send(LSCommand::AddReady(hash, blocks))?;
    }

    system.local_storage_command_tx.send(LSCommand::AddManifest(cache_name, manifest))?;

    Ok((headers, std::str::from_utf8(&body)?.to_owned()))
}

async fn relay_request(
    path: &str,
    method: RequestMethod,
    headers: HeaderMap,
    system: Arc<System>,
) -> Result<(HeaderMap, StatusCode, Vec<u8>)> {
    let uri = format!("{}{path}", system.registry);
    debug!("Relaying traffic to {uri}. ");

    let client = system.client.clone();
    let mut request = match method {
        RequestMethod::Get => client.get(&uri),
    };

    let host_uri = Url::parse(&uri)?;
    let host = host_uri
        .host_str()
        .ok_or(anyhow!("no host found in {host_uri}"))?;

    request = request.header(header::HOST, host);
    for (name, value) in headers {
        let name = match name {
            Some(name) => name,
            None => continue,
        };

        if name == header::HOST {
            // we replaced host
            continue;
        }

        request = request.header(name, value);
    }

    let response = request.send().await?;

    let status = response.status();
    debug!(
        "Remote Docker registry returned {}. ",
        response.status().as_u16()
    );

    let headers = response.headers().clone();
    let body = response.bytes().await?.to_vec();

    Ok((headers, status, body))
}
