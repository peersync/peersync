use std::{collections::HashSet, sync::{Arc, Mutex}, time::Duration};

use anyhow::{anyhow, Error, Result};
use axum::{
    extract::{Extension, Json},
    http::StatusCode,
    response::IntoResponse,
};
use log::{debug, info, warn};
use rand::Rng;
use tokio::{sync::oneshot, time::{sleep, timeout}};

use crate::{
    dht::KBucketCommand,
    node::{Key, Node},
    system::System,
    tracker::{
        client::{
            announce_election_result,
            get_trackers_from_node,
            request_election,
            ping_tracker,
        },
        protocol::*,
        TrackerListCommand,
    },
};

const GET_TRACKER_INIT_TIMEOUT: Duration = Duration::from_secs(5);
const TRACKER_TEST_INTERVAL: Duration = Duration::from_secs(300);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(30);
const EPS: f64 = 10e-3;

pub struct TrackerElectionManager {
    electing: Mutex<bool>,
    my_score: Mutex<f64>,
    current_best: Mutex<Option<(f64, Key)>>,
    system: Arc<System>,
}

impl TrackerElectionManager {
    pub fn new(system: Arc<System>) -> Self {
        Self {
            electing: Mutex::new(false),
            my_score: Mutex::new(calculate_score(&system.local_node)),
            current_best: Mutex::new(None),
            system,
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<()> {
        let get_tracker_nodes = {
            let (tx, rx) = oneshot::channel();
            let command = KBucketCommand::DumpNodes(tx);
            self.system.kb_command_tx.send(command)?;
            rx.await?
        };

        let handles: Vec<_> = get_tracker_nodes.into_iter().map(|node| {
            let client = self.system.client.clone();
            tokio::spawn(async move {
                match timeout(
                    GET_TRACKER_INIT_TIMEOUT,
                    get_trackers_from_node(client, &node),
                ).await {
                    Ok(Ok(res)) => res,
                    Ok(Err(e)) => {
                        warn!("Failed to get trackers from peer {}: {e}. ", node.name);
                        HashSet::new()
                    },
                    Err(e) => {
                        warn!("Failed to get trackers from peer {}: {e}. ", node.name);
                        HashSet::new()
                    },
                }
            })
        }).collect();

        debug!("Asking {} peers for trackers. ", handles.len());

        let mut trackers = HashSet::new();
        for handle in handles {
            let result = handle.await?;
            trackers.extend(result);
        }

        debug!("Received {} trackers. ", trackers.len());

        let (live, dead) = tracker_live_or_dead(trackers, Arc::clone(&self.system)).await;
        debug!("Dumped {} dead trackers. ", dead.len());
        
        for tracker in live.into_iter() {
            self.system.tracker_list_command_tx.send(TrackerListCommand::Add((tracker, None)))?;
        }

        tokio::spawn(async move {
            // sleep first to allow own server to start up
            sleep(Duration::from_secs(1)).await;

            loop {
                let s = Arc::clone(&self);
                if let Err(e) = s.tracker_election_round().await {
                    warn!("Tracker election error: {e}. ");
                }

                sleep(TRACKER_TEST_INTERVAL).await;
            }
        });

        Ok(())
    }

    async fn tracker_election_round(self: Arc<Self>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.system.tracker_list_command_tx.send(TrackerListCommand::GetSeq(tx))?;
        let old_seq = rx.await?;

        let (tx, rx) = oneshot::channel();
        self.system.tracker_list_command_tx.send(TrackerListCommand::Get(tx))?;
        let trackers = rx.await?;
    
        let (live, dead) = tracker_live_or_dead(trackers, Arc::clone(&self.system)).await;
        if !dead.is_empty() {
            debug!("{} trackers dead. ", dead.len());
        }

        for dead_tracker in dead.into_iter() {
            self.system.tracker_list_command_tx.send(TrackerListCommand::Delete(dead_tracker))?;
        }

        let already_electing = self.electing.lock().as_deref().cloned().unwrap_or(false);

        let (tx, rx) = oneshot::channel();
        self.system.tracker_list_command_tx.send(TrackerListCommand::GetSeq(tx))?;
        let new_seq = rx.await?;

        if live.len() == 0 && !already_electing && new_seq == old_seq {
            info!("Requesting tracker election. ");
            let message = ElectionMessage {
                key: self.system.local_node.key.clone(),
                score: 0.0,
            };
            request_election(self.system.client.clone(), message, &self.system.local_node).await?;
        }
    
        Ok(())
    }

    fn finish_election(&self) -> Result<()> {
        *self.electing.lock().map_err(|_| anyhow!("failed to acquire lock"))? = false;
        *self.current_best.lock().map_err(|_| anyhow!("failed to acquire lock"))? = None;
        Ok(())
    }
}

async fn tracker_live_or_dead(
    trackers: HashSet<Node>,
    system: Arc<System>,
) -> (HashSet<Node>, HashSet<Node>) {
    let trackers: Vec<_> = trackers.into_iter().collect();

    let res: Vec<_> = trackers
        .iter()
        .map(|tracker| {
            let client = system.client.clone();
            tokio::spawn(ping_tracker(client, tracker.clone()))
        })
        .collect();

    let mut connectable = vec![];
    for handle in res {
        let res = match handle.await {
            Ok(true) => true,
            _ => false,
        };
        connectable.push(res);
    }

    let mut live = HashSet::new();
    let mut dead = HashSet::new();

    for (i, tracker) in trackers.into_iter().enumerate() {
        if connectable[i] {
            live.insert(tracker);
        } else {
            dead.insert(tracker);
        }
    }

    (live, dead)
}

pub fn calculate_score(node: &Node) -> f64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(0.0..10.0) * node.uptime() as f64
}

pub async fn election_handler(
    Extension(system): Extension<Arc<System>>,
    Extension(em): Extension<Arc<TrackerElectionManager>>,
    Json(message): Json<ElectionMessage>,
) -> impl IntoResponse {
    match election_impl(system, em, message).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
        },
    }
}

async fn election_impl(
    system: Arc<System>,
    election_manager: Arc<TrackerElectionManager>,
    message: ElectionMessage,
) -> Result<()> {
    let already_electing = {
        let mut electing = election_manager.electing.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
        let old_state = *electing;
        *electing = true;
        old_state
    };

    let my_score = {
        let mut my_score = election_manager.my_score.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
        if !already_electing {
            *my_score = calculate_score(&election_manager.system.local_node);
        }
        *my_score
    };

    if !already_electing {
        debug!("Starting new election process. ");

        let current_best = if my_score > message.score {
            (my_score, election_manager.system.local_node.key.clone())
        } else {
            (message.score, message.key)
        };
        *election_manager.current_best.lock().map_err(|_| anyhow!("failed to acquire lock"))? = Some(current_best);

        tokio::spawn(async move {
            sleep(ELECTION_TIMEOUT).await;

            let best_key = match &*election_manager.current_best
                .lock()
                .map_err(|_| anyhow!("failed to acquire lock"))? {
                Some((_, key)) => key.clone(),
                None => return Ok(()),
            };

            if best_key != election_manager.system.local_node.key {
                return Ok(());
            }

            info!("I'm the new tracker! ");

            election_manager.finish_election()?;
            system.tracker.enable()?;

            let broadcast_nodes = {
                let (tx, rx) = oneshot::channel();
                let command = KBucketCommand::DumpNodes(tx);
                system.kb_command_tx.send(command)?;
                rx.await?
            };

            let tracker = Arc::new(system.local_node.clone());
            for node in broadcast_nodes {
                let client = system.client.clone();
                let tracker = Arc::clone(&tracker);
                tokio::spawn(announce_election_result(client, tracker, node));
            }

            Ok::<(), Error>(())
        });
    } else {
        debug!("Continuing with existing process. ");

        let current_best_score = match *election_manager.current_best
            .lock()
            .map_err(|_| anyhow!("failed to acquire lock"))? {
            Some((score, _)) => score,
            None => my_score,
        };

        // ignoring a worse entry
        if f64::abs(current_best_score - message.score) >= EPS && message.score < current_best_score {
            return Ok(());
        }

        // nothing new
        if message.score < current_best_score && current_best_score == my_score {
            return Ok(());
        }

        let message_key = message.key.clone();

        let (new_message, no_send_back) = if f64::abs(my_score - message.score) < EPS {
            debug!("The score I got equals to my own. ");
            if election_manager.system.local_node.key < message.key {
                (ElectionMessage {
                    score: my_score,
                    key: election_manager.system.local_node.key.clone(),
                }, false)
            } else {
                (message, true)
            }
        } else if my_score > message.score {
            debug!("I have higher score! ");
            (ElectionMessage {
                score: my_score,
                key: election_manager.system.local_node.key.clone(),
            }, false)
        } else {
            debug!("I have lower score! ");
            (message, true)
        };

        let current_best = (new_message.score, new_message.key.clone());
        *election_manager.current_best.lock().map_err(|_| anyhow!("failed to acquire lock"))? = Some(current_best);

        let broadcast_nodes = {
            let (tx, rx) = oneshot::channel();
            let command = KBucketCommand::DumpNodes(tx);
            election_manager.system.kb_command_tx.send(command)?;
            rx.await?
        };

        let broadcast_nodes: Vec<_> = broadcast_nodes
            .into_iter()
            .filter(|n| n.key != message_key && !no_send_back)
            .collect();

        for node in broadcast_nodes.into_iter() {
            let message = new_message.clone();
            let client = system.client.clone();
            let kb_command_tx = election_manager.system.kb_command_tx.clone();
            tokio::spawn(async move {
                debug!("Sending tracker election request to {}. ", node.name);
                if let Err(e) = request_election(client, message, &node).await {
                    warn!("Failed to broadcast tracker election message: {e}. ");
                    kb_command_tx.send(KBucketCommand::DeleteNode(node.key))?;
                }
                Ok::<(), Error>(())
            });
        }
    }

    Ok(())
}

pub async fn election_result_handler(
    Extension(system): Extension<Arc<System>>,
    Extension(em): Extension<Arc<TrackerElectionManager>>,
    Json(node): Json<Node>,
) -> impl IntoResponse {
    match election_result_impl(system, em, node).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
        },
    }
}

async fn election_result_impl(
    system: Arc<System>,
    em: Arc<TrackerElectionManager>,
    tracker: Node,
) -> Result<()> {
    let exists = {
        let tracker = tracker.clone();
        let (tx, rx) = oneshot::channel();
        system.tracker_list_command_tx.send(TrackerListCommand::Add((tracker, Some(tx))))?;
        rx.await?
    };

    if exists {
        return Ok(());
    }

    let electing = *em.electing.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
    if !electing {
        // already relayed the message
        return Ok(());
    }

    system.tracker_list_command_tx.send(TrackerListCommand::IncSeq)?;
    em.finish_election()?;

    info!("Node {} is the new tracker! ", tracker.name);

    let broadcast_nodes = {
        let (tx, rx) = oneshot::channel();
        let command = KBucketCommand::DumpNodes(tx);
        system.kb_command_tx.send(command)?;
        rx.await?
    };

    let tracker = Arc::new(tracker);

    for node in broadcast_nodes {
        if node == *tracker {
            continue;
        }

        let client = system.client.clone();
        let tracker = Arc::clone(&tracker);
        tokio::spawn(announce_election_result(client, tracker, node));
    }

    Ok(())
}
