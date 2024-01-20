use reqwest::Client;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    dht::KBucketCommand,
    merkle_tree::MerkleTreeStorage,
    node::Node, peer::PeerCommand,
    parallel_download::LSCommand,
    tracker::{
        Tracker,
        TrackerListCommand,
    },
};

pub struct System {
    pub local_node: Node,
    pub kb_command_tx: UnboundedSender<KBucketCommand>,
    pub peer_command_tx: UnboundedSender<PeerCommand>,
    pub tracker_list_command_tx: UnboundedSender<TrackerListCommand>,
    pub local_storage_command_tx: UnboundedSender<LSCommand>,
    pub tracker: Tracker,
    pub client: Client,
    pub registry: String,
    pub cache_directory: String,
    pub merkle_tree_storage: MerkleTreeStorage,
    pub record: bool,
    pub record_path: String,
}

impl System {
    pub fn new(
        local_node: Node,
        kb_command_tx: UnboundedSender<KBucketCommand>,
        peer_command_tx: UnboundedSender<PeerCommand>,
        tracker_list_command_tx: UnboundedSender<TrackerListCommand>,
        local_storage_command_tx: UnboundedSender<LSCommand>,
        tracker: Tracker,
        client: Client,
        registry: String,
        cache_directory: String,
        record: bool,
        record_path: String,
    ) -> Self {
        let merkle_tree_storage = MerkleTreeStorage::new();

        Self {
            local_node,
            kb_command_tx,
            peer_command_tx,
            tracker,
            tracker_list_command_tx,
            client,
            local_storage_command_tx,
            registry,
            cache_directory,
            merkle_tree_storage,
            record,
            record_path,
        }
    }
}
