use std::{collections::HashMap, path::PathBuf, sync::Mutex};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use log::debug;
use tokio::fs::metadata;

use crate::{
    merkle_tree::r#impl::{AdditionalHashes, MerkleTree, VerifyResult},
    node::Key,
    parallel_download::get_block_size,
};

pub struct MerkleTreeStorage {
    storage: Mutex<HashMap<Key, MerkleTree>>,
}

impl MerkleTreeStorage {
    pub fn new() -> Self {
        Self {
            storage: Mutex::new(HashMap::new()),
        }
    }

    pub async fn load_layer(&self, key: Key, path: &PathBuf) -> Result<()> {
        // check if layer is already complete
        let complete = {
            let storage = self.storage.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
            if let Some(mt) = storage.get(&key) {
                mt.is_complete()
            } else {
                false
            }
        };

        if complete {
            return Ok(());
        }

        let size = metadata(path).await?.len() as usize;

        let block_size = get_block_size(size);
        let block_n = {
            let mut x = size / block_size;
            if size % block_size != 0 {
                x += 1;
            }
            x
        };
        let mut mt = MerkleTree::init(block_n, block_size);

        debug!("Building merkle tree... ");

        let path = path.clone();
        let (res, mt) = tokio::spawn(async move {
            let res = mt.build_from_data(&path).await;
            (res, mt)
        }).await?;

        if let Err(e) = res {
            return Err(e);
        }

        debug!("Loaded layer {key} into merkle tree. ");

        self.storage.lock().map_err(|_| anyhow!("failed to acquire lock"))?.insert(key, mt);

        Ok(())
    }

    pub fn layer_exists(&self, key: &Key) -> Result<bool> {
        let storage = self.storage.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
        Ok(storage.contains_key(key))
    }

    pub fn add_mt(&self, hashes: AdditionalHashes, key: &Key, block_n: usize, block_size: usize) -> Result<()> {
        let mut storage = self.storage.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
        let mt = MerkleTree::from_hashes(hashes, block_n, block_size);
        storage.insert(key.clone(), mt);
        Ok(())
    }

    pub async fn verify_block(
        &self,
        key: &Key,
        block: &Bytes,
        idx: usize,
        additional_hashes: AdditionalHashes,
        block_n: usize,
        block_size: usize,
    ) -> Result<bool> {
        let resp = loop {
            let resp = {
                let mut s = self.storage.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
                let mut mt = match s.get(key) {
                    Some(mt) => mt.clone(),
                    None => MerkleTree::init(block_n, block_size),
                };
                let resp = mt.verify_with(block, idx, &additional_hashes);
                s.insert(key.clone(), mt);
                resp
            };

            let more_blocks = match resp {
                VerifyResult::Good => break true,
                VerifyResult::Bad => break false,
                VerifyResult::NeedMore(blocks) => blocks,
            };

            debug!("Still need {} block(s). ", more_blocks.len());
        };

        Ok(resp)
    }

    pub fn dump(&self, key: &Key) -> Result<Option<AdditionalHashes>> {
        let storage = self.storage.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
        let mt = match storage.get(key) {
            Some(mt) => mt,
            None => return Ok(None),
        };

        Ok(Some(mt.export()))
    }
}
