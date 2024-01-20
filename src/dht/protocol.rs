use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::node::{Key, Distance, Node};

#[derive(Serialize)]
pub enum Request<'a> {
    Ping(&'a Node),
    FindNode(&'a Key),
    Announce(&'a Node, &'a Key),
}

#[derive(Deserialize)]
pub enum IncomingRequest {
    Ping(Node),
    FindNode(Key),
    Announce(Node, Key),
}

#[derive(Serialize)]
pub enum Response<'a> {
    Ping(&'a Node),
    FindNode(&'a Vec<NodeAndDistance>),
    Announce(&'a HashSet<Node>),
}

#[derive(Deserialize)]
pub enum IncomingResponse {
    Ping(Node),
    FindNode(Vec<NodeAndDistance>),
    Announce(HashSet<Node>),
}

#[derive(Serialize, Deserialize, Clone, Hash, Eq, PartialEq, Debug)]
pub struct NodeAndDistance(pub Node, pub Distance);
