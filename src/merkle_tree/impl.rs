use std::{collections::HashMap, fs::metadata, path::PathBuf, sync::{Arc, Mutex}};

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use log::{debug, warn};
use once_cell::sync::OnceCell;
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::{mpsc::{self, Sender}, Semaphore},
    task,
};

#[derive(Clone)]
pub struct MerkleTree {
    block_n: usize,
    block_size: usize,
    hashes: Vec<Option<[u8; 32]>>, // sha256
}

#[derive(Deserialize, Serialize)]
pub struct AdditionalHashes(pub Vec<(usize, [u8; 32])>);

pub enum VerifyResult {
    Good,
    Bad,
    NeedMore(Vec<usize>), // additional positions
}

impl MerkleTree {
    pub fn init(block_n: usize, block_size: usize) -> Self {
        let zero_hash = get_zero_block(block_size);

        let n_leaf_nodes = {
            let mut cnt = 1;
            while cnt < block_n {
                cnt *= 2;
            }
            cnt
        };

        let leaf_nodes = {
            let mut nodes = vec![];

            for _ in 0..block_n {
                nodes.push(None);
            }

            while nodes.len() != n_leaf_nodes {
                nodes.push(Some(zero_hash.clone()));
            }

            nodes
        };

        let mut hashes = vec![None; n_leaf_nodes - 1];
        hashes.extend(leaf_nodes);

        debug!("Initialized merkle tree with size {}. ", hashes.len());

        Self {
            block_n,
            block_size,
            hashes,
        }
    }

    pub fn from_hashes(hashes: AdditionalHashes, block_n: usize, block_size: usize) -> Self {
        let mut mt = Self::init(block_n, block_size);
        for (i, hash) in hashes.0.into_iter() {
            mt.hashes[i] = Some(hash);
        }
        mt
    }

    pub fn export(&self) -> AdditionalHashes {
        let mut hashes = vec![];
        for (i, hash) in self.hashes.iter().enumerate() {
            if let Some(hash) = hash {
                hashes.push((i, *hash));
            }
        }
        AdditionalHashes(hashes)
    }

    pub fn is_complete(&self) -> bool {
        self.hashes.iter().all(|hash| hash.is_some())
    }

    pub async fn build_from_data(&mut self, path: &PathBuf) -> Result<()> {
        // check content length
        let blocks = {
            let total_size = metadata(path)?.len() as usize;
            let mut x = total_size / self.block_size;
            if total_size % self.block_size != 0 {
                x += 1;
            }
            x
        };
        if blocks != self.block_n {
            return Err(anyhow!("bad content length, expected {} block(s), got {} block(s)", self.block_n, blocks));
        }

        let max_concurrent_tasks = 4;

        // compute hashes
        let hashes = {
            let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));
            let mut tasks = Vec::new();

            let (tx, mut rx) = mpsc::channel(64);

            let block_size = self.block_size;
            let reader = async move |path: PathBuf, tx: Sender<Bytes>| {
                let mut file = File::open(path).await?;
                let mut buf = vec![0u8; block_size];

                while let Ok(len) = file.read(&mut buf).await {
                    if len == 0 {
                        break;
                    }

                    let block = Bytes::copy_from_slice(&buf[..len]);
                    tx.send(block).await?;
                }

                Ok::<(), Error>(())
            };

            // the reader thread
            let path = path.clone();
            tokio::spawn(async move {
                if let Err(e) = reader(path, tx).await {
                    warn!("Failed to build merkle tree: {e}. ")
                }
            });

            while let Some(block) = rx.recv().await {
                let permit = semaphore.clone().acquire_owned().await?;
                let task = task::spawn_blocking(move || {
                    let hash = sha256(&block);
                    drop(permit); // release the semaphore permit
                    hash
                });
                tasks.push(task);
            }

            // collect all hashes
            let mut hashes = Vec::new();
            for task in tasks {
                match task.await {
                    Ok(hash) => hashes.push(hash),
                    Err(e) => return Err(anyhow!("failed to compute hash: {:?}", e)),
                }
            }

            hashes
        };


        // update leaf nodes
        let leaf_start = self.hashes.len() / 2; // full binary tree
        self.hashes
            .iter_mut()
            .skip(leaf_start)
            .zip(hashes.into_iter())
            .for_each(|(node, hash)| {
                *node = Some(hash);
            });

        // update non-leaf nodes
        let mut current_idx = leaf_start - 1;
        while current_idx > 0 {
            let left_child_idx = current_idx * 2 + 1;
            let right_child_idx = current_idx * 2 + 2;

            // children are already computed so unwraping is safe
            let left_child_hash = self.hashes[left_child_idx].unwrap();
            let right_child_hash = self.hashes[right_child_idx].unwrap();

            let combined = [left_child_hash.as_ref(), right_child_hash.as_ref()].concat();

            let hash = sha256(&combined);

            self.hashes[current_idx] = Some(hash);

            current_idx -= 1;
        }

        // finally, the root node
        let left_child_hash = self.hashes[1].unwrap();
        let right_child_hash = self.hashes[2].unwrap();
        let combined = [left_child_hash.as_ref(), right_child_hash.as_ref()].concat();
        let hash = sha256(&combined);
        self.hashes[0] = Some(hash);

        Ok(())
    }

    pub fn verify_with(&mut self, block: &Bytes, idx: usize, additional_hashes: &AdditionalHashes) -> VerifyResult {
        if block.len() > self.block_size {
            // too long
            return VerifyResult::Bad;
        }

        // the pending tree
        let new_hashes = {
            let mut new_hashes = self.hashes.clone();

            for (idx, hash) in &additional_hashes.0 {
                new_hashes[*idx] = Some(*hash);
            }

            let block_hash = sha256(&block);
            new_hashes[idx] = Some(block_hash);

            new_hashes
        };

        // check if we have enough hashes
        let missing = {
            let mut missing = vec![];

            let mut current_idx = idx + new_hashes.len() / 2;

            while current_idx > 0 {
                let sibling_idx = if current_idx % 2 == 0 {
                    current_idx - 1
                } else {
                    current_idx + 1
                };

                if new_hashes[sibling_idx].is_none() {
                    missing.push(sibling_idx);
                }

                current_idx = (current_idx - 1) / 2;
            }

            missing
        };

        if !missing.is_empty() {
            return VerifyResult::NeedMore(missing);
        }

        // we have enough hashes, now do the verification
        let mut current_idx = idx + new_hashes.len() / 2;
        // start with the hash of the block
        let mut current_hash = new_hashes[current_idx].unwrap();

        while current_idx > 0 {
            if new_hashes[current_idx].is_some() {
                // found valid sig, exit early
                break;
            }

            let parent_idx = (current_idx - 1) / 2;
            let sibling_idx = if current_idx % 2 == 0 {
                current_idx - 1
            } else {
                current_idx + 1
            };

            let sibling_hash = new_hashes[sibling_idx].unwrap();

            // combine the current hash and sibling hash
            let combined = if current_idx % 2 == 0 {
                [current_hash.as_ref(), sibling_hash.as_ref()].concat()
            } else {
                [sibling_hash.as_ref(), current_hash.as_ref()].concat()
            };

            // compute the hash of the combination
            current_hash = sha256(&combined);

            // move up the tree
            current_idx = parent_idx;
        }

        // check if the computed hash matches the root hash
        if current_hash != self.hashes[current_idx].unwrap() {
            return VerifyResult::Bad;
        }

        // update the hashes
        self.hashes = new_hashes;

        VerifyResult::Good
    }
}

fn get_zero_block(size: usize) -> [u8; 32] {
    static BLOCKS: OnceCell<Mutex<HashMap<usize, [u8; 32]>>> = OnceCell::new();
    let blocks = BLOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut blocks = blocks.lock().unwrap();

    let block = match blocks.get(&size) {
        Some(block) => block.clone(),
        None => {
            let zero_block = vec![0u8; size];
            let zero_hash: [u8; 32] = sha256(&zero_block);
            blocks.insert(size, zero_hash);
            zero_hash
        },
    };

    block
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut context = Context::new(&SHA256);
    context.update(data);
    let digest = context.finish();
    digest.as_ref().try_into().unwrap()
}
