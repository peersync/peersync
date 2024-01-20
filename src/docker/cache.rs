use std::{
    fs::{metadata, File},
    io::{BufReader, Read},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{anyhow, Error, Result};
use data_encoding::HEXUPPER;
use futures::executor::block_on;
use glob::glob;
use log::{debug, info, warn};
use rayon::iter::{IntoParallelIterator, ParallelIterator, IndexedParallelIterator};
use ring::digest::{Context, Digest, SHA256};
use tokio::sync::oneshot;

use crate::{
    dht::method::announce,
    node::Key,
    parallel_download::LSCommand,
    system::System,
    tracker::{announce_to_trackers, TrackerListCommand},
    util::*,
};

fn sha256_digest<R: Read>(mut reader: R) -> Result<Digest> {
    let mut context = Context::new(&SHA256);
    let mut buffer = vec![0u8; 64 * 1024 * 1024]; // 64 MiB

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

    Ok(context.finish())
}

pub async fn init_cache(system: Arc<System>) -> Result<()> {
    let mut entries = vec![];
    let mut systems = vec![];
    for entry in glob(&format!("{}/*", system.cache_directory))? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                warn!("Failed to read cache file {}: {e}. ", e.path().display());
                continue;
            }
        };

        debug!("Inspecting cache file {}. ", entry.display());

        entries.push(entry);
        systems.push(Arc::clone(&system));
    }

    let entries: Vec<_> = entries
        .into_par_iter()
        .zip(systems.into_par_iter())
        .map(|(entry, system)| (block_on(process_file(&entry, system)), entry))
        .filter(|(res, entry)| {
            if let Err(e) = res {
                warn!("Failed to process cache file {}: {e}. ", entry.display());
            }
            res.is_ok()
        })
        // Err variants are already filtered out so unwraping is safe
        .map(|(res, _)| res.unwrap())
        .collect();

    for (key, size) in entries {
        info!("Found existing layer: {}. ", key);

        let system = Arc::clone(&system);

        let key_announce = key.clone();
        let system_announce = Arc::clone(&system);
        tokio::spawn(async move {
            // We already have the cache file so there's no need to keep announce result
            if let Err(e) = announce(&key_announce, system_announce).await {
                warn!("Failed to announce {key_announce}: {e}. ");
            }
        });

        system.local_storage_command_tx.send(LSCommand::SetSize(key.clone(), size))?;

        // if byte 0 ~ (4MiB - 1): we have block 0 in full
        // len = 4MiB at that time
        let blocks = byte_to_block(0, size - 1, size);
        system
            .local_storage_command_tx
            .send(LSCommand::AddReady(key, blocks))?;
    }

    Ok(())
}

async fn process_file(entry: &PathBuf, system: Arc<System>) -> Result<(Key, usize)> {
    let hash = entry
        .components()
        .last()
        .ok_or(anyhow!("invalid path {}", entry.display()))?
        .as_os_str();
    let hash = hash
        .to_str()
        .ok_or(anyhow!("invalid path {}", entry.display()))?;
    let key = Key::from_str(hash)?;

    let file = File::open(&entry)?;
    let size = metadata(&entry)?.len();

    // long sync process
    // todo
    let calculated_hash = std::thread::spawn(|| {
        let reader = BufReader::new(file);
        let digest = sha256_digest(reader)?;
        Ok::<String, Error>(HEXUPPER.encode(digest.as_ref()))
    }).join().unwrap()?;

    if calculated_hash != hash {
        return Err(anyhow!(
            "cache file {} possibly corrupted (hash mismatch)",
            entry.display(),
        ));
    }

    let trackers: Vec<_> = {
        let (tx, rx) = oneshot::channel();
        let tc = TrackerListCommand::Get(tx);
        system.tracker_list_command_tx.send(tc)?;
        rx.await?.into_iter().collect()
    };

    let system_tk = Arc::clone(&system);
    let key_tk = key.clone();
    tokio::spawn(async move {
       announce_to_trackers(&trackers, &key_tk, true, system_tk).await
    });

    // now generate the merkle tree
    system.merkle_tree_storage.load_layer(key.clone(), entry).await?;

    Ok((key, size as usize))
}
