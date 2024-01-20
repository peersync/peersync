use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::node::{Key, Node};

#[derive(Serialize)]
pub struct GetRequest<'a> {
    pub key: &'a Key,
    pub node: Option<&'a Node>,
}

#[derive(Deserialize)]
pub struct IncomingGetRequest {
    pub key: Key,
    pub node: Option<Node>,
}

#[derive(Serialize, Deserialize)]
pub struct GetResponse {
    pub peers: HashSet<Node>,
}

#[derive(Serialize)]
pub struct OfferRequest<'a> {
    key: &'a Key,
    node: &'a Node,
}

#[derive(Deserialize)]
pub struct IncomingOfferRequest {
    pub key: Key,
    pub node: Node,
}

#[derive(Deserialize, Serialize)]
pub struct GetTrackersResponse {
    pub trackers: HashSet<Node>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ElectionMessage {
    pub score: f64,
    pub key: Key,
}
