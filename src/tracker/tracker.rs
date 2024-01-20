use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Result};
use axum::{
    extract::{Extension, Json},
    http::{header, StatusCode},
    response::IntoResponse,
};
use chrono::Utc;
use log::{debug, warn};
use tokio::{sync::oneshot, time::sleep};

use crate::{
    node::{Key, Node},
    system::System,
    tracker::{protocol::*, TrackerListCommand},
};

const CLEAN_INTERVAL: i64 = 300;

// the second parameter is timestamp
#[derive(Clone, Debug)]
struct Peer(Node, i64);

pub struct Tracker {
    storage: Mutex<HashMap<Key, HashSet<Peer>>>,
    enabled: Mutex<bool>,
}

impl PartialEq for Peer {
    fn ne(&self, other: &Self) -> bool {
        self.0 != other.0
    }

    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for Peer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Eq for Peer {}

impl Default for Tracker {
    fn default() -> Self {
        Self {
            storage: Mutex::new(HashMap::new()),
            enabled: Mutex::new(false),
        }
    }
}

impl Tracker {
    pub async fn start_cleaner(&self) {
        loop {
            sleep(Duration::from_secs(CLEAN_INTERVAL as u64)).await;

            let enabled = match self.enabled.lock() {
                Ok(enabled) => *enabled,
                Err(e) => {
                    warn!("Tracker state indicator in bad state: {e}. ");
                    continue;
                },
            };

            if !enabled {
                continue;
            }

            let mut storage = match self.storage.lock() {
                Ok(storage) => storage,
                Err(e) => {
                    warn!("Tracker storage indicator in bad state: {e}. ");
                    continue;
                },  
            };

            let old_size: usize = storage.iter().map(|b| b.1.len()).sum();

            for bucket in &mut *storage {
                bucket.1.retain(|peer| {
                    let now = Utc::now().timestamp();
                    now - peer.1 <= CLEAN_INTERVAL
                });
            }

            let new_size: usize = storage.iter().map(|b| b.1.len()).sum();
            debug!("Tracker cleaner cleaned {} peer(s). ", old_size - new_size);
        }
    }

    pub fn enable(&self) -> Result<()> {
        *self.enabled.lock().map_err(|_| anyhow!("failed to acquire lock"))? = true;
        Ok(())
    }
}

pub async fn ping_handler(
    Extension(system): Extension<Arc<System>>,
) -> impl IntoResponse {
    match system.tracker.enabled.lock() {
        Ok(enabled) => {
            if *enabled {
                StatusCode::OK.into_response()
            } else {
                let message = format!("Tracker is not open! ");
                (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
            }
        }
        Err(e) => {
            let message = format!("Failed to get tracker status: {e} ");
            (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
        },
    }
}

pub async fn offer_handler(
    Extension(system): Extension<Arc<System>>,
    Json(request): Json<IncomingOfferRequest>,
) -> impl IntoResponse {
    let res = match offer_impl(&system.tracker, request) {
        Ok(res) => res,
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        },
    };

    if res {
        StatusCode::OK.into_response()
    } else {
        let message = format!("Tracker is not open! ");
        (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
    }
}

fn offer_impl(tracker: &Tracker, request: IncomingOfferRequest) -> Result<bool> {
    if !*tracker.enabled.lock().map_err(|_| anyhow!("failed to acquire lock"))? {
        return Ok(false);
    }

    let peer = Peer(request.node, Utc::now().timestamp());

    tracker.storage
        .lock()
        .map_err(|_| anyhow!("failed to acquire lock"))?
        .entry(request.key)
        .or_insert(HashSet::new())
        .replace(peer);

    Ok(true)
}

pub async fn get_handler(
    Extension(system): Extension<Arc<System>>,
    Json(request): Json<IncomingGetRequest>,
) -> impl IntoResponse {
    let response = match get_impl(&system.tracker, request) {
        Ok(res) => res,
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        },
    };

    let response = match response {
        Some(response) => response,
        None => {
            let message = format!("Tracker is not open! ");
            return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        },
    };

    (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], response).into_response()
}

fn get_impl(tracker: &Tracker, request: IncomingGetRequest) -> Result<Option<String>> {
    if !*tracker.enabled.lock().map_err(|_| anyhow!("failed to acquire lock"))? {
        return Ok(None);
    }

    debug!("Received tracker get request for {}. ", request.key);

    let storage = tracker.storage
        .lock()
        .map_err(|_| anyhow!("failed to acquire lock"))?
        .get(&request.key)
        .cloned();

    let response: HashSet<_> = storage
        .unwrap_or(HashSet::new())
        .into_iter()
        .map(|peer| peer.0)
        .filter(|peer| match &request.node {
            Some(node) => node != peer,
            None => true,
        })
        .collect();
    let response = response.clone();

    let response = GetResponse {
        peers: response,
    };

    let response = serde_json::to_string(&response)?;

    if let Some(node) = request.node {
        let peer = Peer(node, Utc::now().timestamp());

        tracker.storage
            .lock()
            .map_err(|_| anyhow!("failed to acquire lock"))?
            .entry(request.key)
            .or_insert(HashSet::new())
            .replace(peer);
    }

    Ok(Some(response))
}

pub async fn get_tracker_handler(
    Extension(system): Extension<Arc<System>>,
) -> impl IntoResponse {
    let res = match get_tracker_impl(system).await {
        Ok(res) => res,
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            return (StatusCode::INTERNAL_SERVER_ERROR, message).into_response();
        },
    };

    (StatusCode::OK, res).into_response()
}

async fn get_tracker_impl(system: Arc<System>) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::channel();
    system.tracker_list_command_tx.send(TrackerListCommand::Get(resp_tx))?;

    let response = resp_rx.await?;
    debug!("Returning {} trackers. ", response.len());

    let response = GetTrackersResponse {
        trackers: response,
    };
    let response = serde_json::to_string(&response)?;

    Ok(response)
}
