use std::{net::SocketAddr, sync::Arc, time::Instant};

use anyhow::{anyhow, Error, Result};
use axum::{
    body::Body,
    extract::Extension,
    http::{header, Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{any, get},
    Router,
    Server,
};
use axum_server::tls_rustls::RustlsConfig;
use tokio::{fs::OpenOptions, io::AsyncWriteExt, task::JoinSet};
use lazy_static::lazy_static;
use log::warn;
use once_cell::sync::OnceCell;

use crate::{
    dht::dht_handler,
    docker::{
        docker_base_handler,
        docker_image_handler,
        docker_manifest_handler,
    },
    merkle_tree::merkle_tree_req_handler,
    parallel_download::block_status_handler,
    system::System,
    tracker::{
        tracker_election_handler,
        tracker_election_result_handler,
        tracker_get_handler,
        tracker_get_tracker_handler,
        tracker_offer_handler,
        tracker_ping_handler,
        TrackerElectionManager,
    },
};

static RECORD_PATH: OnceCell<String> = OnceCell::new();

lazy_static!(
    static ref START_TIME: Instant = Instant::now();
);

pub async fn start_server(
    listen: &Vec<SocketAddr>,
    tls: bool,
    certificate: Option<String>,
    privkey: Option<String>,
    system: Arc<System>,
    election_manager: Arc<TrackerElectionManager>,
) -> Result<()> {
    let record = system.record;
    let record_path = system.record_path.clone();

    let mut app = Router::new()
        .route("/dht", get(dht_handler))
        .route("/tracker/offer", get(tracker_offer_handler))
        .route("/tracker/get", get(tracker_get_handler))
        .route("/tracker/get_trackers", get(tracker_get_tracker_handler))
        .route("/tracker/ping", get(tracker_ping_handler))
        .route("/tracker/start_election", get(tracker_election_handler))
        .route("/tracker/election_result", get(tracker_election_result_handler))
        .route("/v2", any(docker_base_handler))
        .route(
            "/v2/:name/manifests/:reference",
            get(docker_manifest_handler).head(docker_manifest_handler),
        )
        .route("/v2/:name/blobs/:digest", get(docker_image_handler))
        .route(
            "/v2/:repo/:name/manifests/:reference",
            get(docker_manifest_handler).head(docker_manifest_handler),
        )
        .route("/v2/:repo/:name/blobs/:digest", get(docker_image_handler))
        .route("/peer/block_status", get(block_status_handler))
        .route("/mt/req", get(merkle_tree_req_handler))
        .layer(Extension(system))
        .layer(Extension(election_manager));

    if record {
        RECORD_PATH.get_or_init(|| {
            let record_path = record_path.as_str();
            record_path.to_string()
        });
        app = app.layer(middleware::from_fn(request_logger));
    }

    let mut futures = JoinSet::new();

    for addr in listen {
        let app = app.clone();
        let addr = addr.clone();
        let serve = app.into_make_service_with_connect_info::<SocketAddr>();

        if tls {
            let certificate = certificate.as_ref().ok_or(anyhow!("config.certificate is not set"))?;
            let privkey = privkey.as_ref().ok_or(anyhow!("config.privkey is not set"))?;
            let config = RustlsConfig::from_pem_file(certificate, privkey).await?;
            futures.spawn(async move {
                axum_server::bind_rustls(addr, config)
                    .serve(serve)
                    .await?;
                Ok::<(), Error>(())
            });
        } else {
            futures.spawn(async move {
                let server = Server::bind(&addr);
                server
                    .serve(serve)
                    .await?;
                Ok::<(), Error>(())
            });
        }
    }

    if let Some(result) = futures.join_next().await {
        let result = result?;
        if let Err(e) = result {
            warn!("Server failed: {e}. ");
        }
    }

    Ok(())
}

async fn request_logger(
    request: Request<Body>,
    next: Next<Body>,
) -> impl IntoResponse {
    let headers = request.headers();
    let content_type = match headers.get(header::CONTENT_TYPE) {
        Some(content_type) => match content_type.to_str() {
            Ok(content_type) => content_type,
            Err(_) => return next.run(request).await,
        },
        None => return next.run(request).await,
    };

    if content_type != "application/json" {
        return next.run(request).await;
    }

    let (req_parts, req_body) = request.into_parts();

    let bytes = match hyper::body::to_bytes(req_body).await {
        Ok(bytes) => bytes,
        Err(e) => {
            let message = format!("Failed to read request body: {e}. ");
            warn!("{message}");
            return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        },
    };

    let data = match std::str::from_utf8(&bytes) {
        Ok(data) => data,
        Err(e) => {
            let message = format!("Failed to parse request body: {e}. ");
            warn!("{message}");
            return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        },
    };

    let mut entry = String::new();
    entry.push_str(&format!("{}\n", START_TIME.elapsed().as_secs()));
    entry.push_str(&format!("{} {}\n", req_parts.method, req_parts.uri));
    entry.push_str(&format!("{}\n\n", data));

    // write to record file
    let record_path = RECORD_PATH.wait();
    let mut file = match OpenOptions::new()
        .append(true)
        .create(true)
        .open(record_path)
        .await {
            Ok(file) => file,
            Err(e) => {
                let message = format!("Failed to open record file: {e}. ");
                warn!("{message}");
                return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
            },
        };

    if let Err(e) = file.write_all(entry.as_bytes()).await {
        let message = format!("Failed to write to record file: {e}. ");
        warn!("{message}");
        return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
    }

    let req_body = hyper::Body::from(bytes);

    let request = Request::from_parts(req_parts, req_body);

    next.run(request).await
}
