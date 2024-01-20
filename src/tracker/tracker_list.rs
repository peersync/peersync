use std::collections::HashSet;

use anyhow::{anyhow, Result};
use log::warn;
use tokio::sync::{mpsc::UnboundedReceiver, oneshot::Sender};

use crate::node::Node;

pub struct TrackerList;

pub enum TrackerListCommand {
    Get(Sender<HashSet<Node>>),
    Add((Node, Option<Sender<bool>>)),
    Delete(Node),
    GetSeq(Sender<u64>),
    IncSeq,
}

impl TrackerList {
    pub async fn start(mut command_rx: UnboundedReceiver<TrackerListCommand>) -> Result<()> {
        let mut trackers = HashSet::new();
        let mut seq = 0;

        while let Some(command) = command_rx.recv().await {
            match command {
                TrackerListCommand::Get(tx) => Self::get_impl(&trackers, tx)?,
                TrackerListCommand::Add((node, tx)) => Self::add_impl(&mut trackers, node, tx)?,
                TrackerListCommand::Delete(node) => Self::del_impl(&mut trackers, node)?,
                TrackerListCommand::GetSeq(tx) => Self::get_seq_impl(seq, tx)?,
                TrackerListCommand::IncSeq => Self::inc_seq_impl(&mut seq)?,
            }
        }

        Ok(())
    }

    fn get_impl(trackers: &HashSet<Node>, tx: Sender<HashSet<Node>>) -> Result<()> {
        tx.send(trackers.clone()).map_err(|_| anyhow!("failed to send response"))?;
        Ok(())
    }

    fn add_impl(trackers: &mut HashSet<Node>, tracker: Node, tx: Option<Sender<bool>>) -> Result<()> {
        let exists = trackers.replace(tracker).is_some();
        if let Some(tx) = tx {
            tx.send(exists).map_err(|_| anyhow!("failed to send response"))?;
        }
        Ok(())
    }

    fn del_impl(trackers: &mut HashSet<Node>, tracker: Node) -> Result<()> {
        if !trackers.remove(&tracker) {
            warn!("Trying to delete a non-existent tracker {}. ", tracker.name);
        }
        Ok(())
    }

    fn get_seq_impl(seq: u64, tx: Sender<u64>) -> Result<()> {
        tx.send(seq).map_err(|_| anyhow!("failed to send response"))?;
        Ok(())
    }

    fn inc_seq_impl(seq: &mut u64) -> Result<()> {
        *seq += 1;
        Ok(())
    }
}
