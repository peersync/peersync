use std::{
    collections::{HashSet, VecDeque},
    io::SeekFrom,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{anyhow, Result};
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;
use log::{debug, warn};
use rand::{distributions::{Distribution, WeightedIndex}, thread_rng};
use reqwest::header;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc::UnboundedSender, oneshot::{self, Sender}},
};

use crate::{
    merkle_tree::{request_hashes, AdditionalHashes},
    node::Key,
    parallel_download::{
        get_block_size,
        local_storage::Range,
        peer_aggregator::{PeerAggregator, PeerInformation},
        LSCommand,
    },
    system::System,
    util::*,
};

const PEER_UPDATE_INTERVAL: u64 = 2000; // in ms
const PARALLEL_NUM: usize = 8;
const MAX_REDIRECTION: u64 = 10;

pub async fn download(
    name: String,
    hash: Key,
    resp_tx: Sender<(u16, HeaderMap, Bytes)>,
    data_tx: UnboundedSender<Result<Bytes>>,
    system: Arc<System>,
) -> Result<()> {
    let size = {
        let (tx, rx) = oneshot::channel();
        system.local_storage_command_tx.send(LSCommand::GetSize(hash.clone(), tx))?;
        rx.await?.unwrap()
    };

    let pa = PeerAggregator::init(
        hash.clone(),
        PEER_UPDATE_INTERVAL,
        system.clone(),
    ).await?;

    let peers = Arc::clone(&pa.peers);

    let peer_found = {
        let len = peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.len();
        len != 0
    };

    if !peer_found {
        let hash_str = format!("{hash}").to_ascii_lowercase();
        let uri = format!("{}/v2/{name}/blobs/sha256:{hash_str}", system.registry);
        debug!("Downloading {name} from registry. ");
        do_download(hash, &uri, resp_tx, data_tx, system).await?;
        return Ok(());
    }

    let block_range = byte_to_block(0, size, size);
    let block_size = get_block_size(size);

    // init merkle tree if not exists
    if !system.merkle_tree_storage.layer_exists(&hash)? {
        let system_req = Arc::clone(&system);
        let target = peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.iter().next().cloned().unwrap().node;
        let hashes = request_hashes(&hash, &target, system_req).await?;
        system.merkle_tree_storage.add_mt(hashes, &hash, block_range.1 + 1, block_size)?;
    }

    // we will retrive the resource from DHT network
    debug!("Downloading {name} from DHT. ");

    let (fin_tx, fin_rx) = oneshot::channel();
    let system_upd = Arc::clone(&system);
    tokio::spawn(async move {
        if let Err(e) = pa.go(PEER_UPDATE_INTERVAL, fin_rx, system_upd).await {
            warn!("Failed to update peers: {e}. ");
        }
    });

    // create cache directory if not exists
    tokio::fs::create_dir_all(&system.cache_directory).await?;

    // create an empty file
    let file_path = format!("{}/{hash}", system.cache_directory);
    let mut file = File::create(&file_path).await?;
    file.set_len(size as u64).await?;

    let mut headers = HeaderMap::new();
    headers.append(header::CONTENT_LENGTH, HeaderValue::from(size));
    resp_tx.send((200, headers, Bytes::new())).map_err(|_| anyhow!("failed to send response"))?;

    let mut download_queue: VecDeque<_> = (block_range.0..=block_range.1).into_iter().collect();

    let mut ok_blocks = vec![None; (size as f64 / block_size as f64).ceil() as usize];
    let mut response_ptr = 0;

    let name = Arc::new(name);

    while !download_queue.is_empty() {
        let mut threads = vec![];
        let mut new_blocks = vec![];

        for _ in 0..PARALLEL_NUM {
            let block = match download_queue.pop_front() {
                Some(block) => block,
                None => break,
            };

            let hash = hash.clone();
            let peers = Arc::clone(&peers);
            let system = Arc::clone(&system);
            let name = Arc::clone(&name);

            threads.push((block, tokio::spawn(p2p_download(name, hash, block, peers, system))));
        }

        for (idx, thread) in threads {
            let block = match thread.await? {
                Ok(block) => block,
                Err(e) => {
                    debug!("Failed to download block {idx}: {e}. ");
                    download_queue.push_back(idx);
                    continue;
                },
            };

            // size validation
            if idx != block_range.1 && block.len() != block_size {
                debug!("Block {idx} has invalid size {}. ", block.len());
                download_queue.push_back(idx);
                continue;
            }

            // hash validation
            let ah = AdditionalHashes(vec![]);
            let res = system.merkle_tree_storage.verify_block(
                &hash,
                &block,
                idx,
                ah,
                block_range.1,
                block_size,
             ).await?;

            if !res {
                debug!("Block {idx} failed hash test. ");
                download_queue.push_back(idx);
                continue;
            }

            debug!("Block {idx} passed test. ");

            file.seek(SeekFrom::Start((idx * block_size) as u64)).await?;
            file.write_all(&block).await?;

            ok_blocks[idx] = Some(block);
            new_blocks.push(idx);
        }

        while response_ptr < ok_blocks.len() && ok_blocks[response_ptr].is_some() {
            let block = ok_blocks[response_ptr].take().unwrap();
            data_tx.send(Ok(block))?;
            response_ptr += 1;
        }

        if new_blocks.len() == 0 {
            continue;
        }

        // add ready blocks to list
        // find consecutive ranges
        let mut range_start = new_blocks[0];
        let mut range_end = new_blocks[0];
        let mut open = true;
        for block in new_blocks.into_iter().skip(1) {
            if block != range_start + 1 {
                // new range
                let range = Range(range_start, range_end);
                system.local_storage_command_tx.send(LSCommand::AddReady(hash.clone(), range))?;
                open = false;
                range_start = block;
            } else {
                open = true;
            }

            range_end = block;
        }

        if open {
            // last range
            let range = Range(range_start, range_end);
            system.local_storage_command_tx.send(LSCommand::AddReady(hash.clone(), range))?;
        }
    }

    fin_tx.send(()).map_err(|_| anyhow!("failed to send signal"))?;

    Ok(())
}

async fn p2p_download(
    name: Arc<String>,
    hash: Key,
    block_id: usize,
    peers: Arc<Mutex<HashSet<PeerInformation>>>,
    system: Arc<System>,
) -> Result<Bytes> {
    let peers = peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.clone();

    let peers: Vec<_> = peers
        .into_iter()
        .filter(|peer| peer.ranges.iter().any(|r| r.has_block(block_id)))
        .collect();

    let scores: Vec<_> = peers.iter().map(|p| p.score).collect();
    debug!("Scores: {scores:?}. ");
    let peers: Vec<_> = peers.into_iter().map(|p| p.node).collect();

    if peers.len() == 0 {
        return Err(anyhow!("no peer available"));
    }

    let peer = {
        let dist = WeightedIndex::new(&scores)?;
        let mut rng = thread_rng();
        &peers[dist.sample(&mut rng)]
    };

    let hash_str = format!("{hash}").to_ascii_lowercase();
    let addr = addr_from_node(peer).ok_or(anyhow!("peer {} has no address", peer.name))?;
    let uri = format!("{}://{addr}/v2/{name}/blobs/sha256:{hash_str}", proto_from_node(peer));

    let client = system.client.clone();
    let response = client
        .get(&uri)
        .header("X_REQUEST_BLOCK", block_id)
        .send()
        .await?;

    let download_start_time = Instant::now();

    if response.status().as_u16() != 200 {
        let code = response.status().as_u16();
        let message = response.text().await?;
        return Err(anyhow!("remote peer returned {code} when requested with block {block_id}: {message}"));
    }

    let block = response.bytes().await?;

    let download_time = download_start_time.elapsed().as_millis() as f64; // in ms
    let size = block.len() as f64;
    let speed = 1000.0 * (size / download_time); // in B/s
    system.local_storage_command_tx.send(LSCommand::AddSpeedDataPoint(peer.key.clone(), speed))?;

    debug!("Downloaded block {} from peer {}. ", block_id, peer.name);

    Ok(block)
}

async fn do_download(
    hash: Key,
    uri: &str,
    resp_tx: Sender<(u16, HeaderMap, Bytes)>,
    data_tx: UnboundedSender<Result<Bytes>>,
    system: Arc<System>,
) -> Result<()> {
    let mut uri = String::from(uri);

    let mut iter = 0;
    let mut response = loop {
        if iter == MAX_REDIRECTION {
            resp_tx
                .send((
                    500,
                    HeaderMap::new(),
                    Bytes::from_static("Max redirection reached. ".as_bytes(),
                )))
                .map_err(|_| anyhow!("failed to send response"))?;
            return Err(anyhow!("max redirection reached"));
        }

        let client = system.client.clone();
        let response = client
            .get(&uri)
            .send()
            .await?;

        let status = response.status();
        let headers = response.headers();

        if status.is_redirection() {
            uri = String::from(headers.get(header::LOCATION).ok_or(anyhow!("invalid redirect response"))?.to_str()?);
        } else {
            break response;
        }

        iter += 1;
    };

    let status = response.status();
    let headers = response.headers().clone();

    if !status.is_success() {
        resp_tx
            .send((status.as_u16(), headers, response.bytes().await?))
            .map_err(|_| anyhow!("failed to send response"))?;
        return Ok(());
    }

    let full_size: usize = headers
        .get(header::CONTENT_LENGTH)
        .ok_or(anyhow!("no size found"))?
        .to_str()?
        .parse()?;

    let download_start_time = Instant::now();

    // create cache directory if not exists
    tokio::fs::create_dir_all(&system.cache_directory).await?;

    // create an empty file
    let file_path = format!("{}/{hash}", system.cache_directory);
    let mut file = File::create(&file_path).await?;

    system.local_storage_command_tx.send(LSCommand::SetSize(hash.clone(), full_size))?;

    resp_tx
        .send((status.as_u16(), headers, Bytes::new()))
        .map_err(|_| anyhow!("failed to send response"))?;

    let mut write_pos = 0;
    loop {
        let chunk = response.chunk().await;

        let chunk = match chunk {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(_) => continue,
        };

        file.seek(SeekFrom::Start(write_pos as u64)).await?;
        file.write_all(&chunk).await?;
        write_pos += chunk.len();

        data_tx.send(Ok(chunk))?;

        if write_pos == full_size {
            break;
        }  
    }

    let download_time = download_start_time.elapsed().as_millis() as f64; // in ms
    let full_size_f64 = full_size as f64; // in B
    let speed = 1000.0 * (full_size_f64 / download_time); // in B/s
    debug!("Downloading finished at speed {speed:.2} bytes/s. ");

    let range = byte_to_block(0, full_size - 1, full_size);
    system.local_storage_command_tx.send(LSCommand::AddReady(hash.clone(), range))?;
    // todo: announce to tracker

    // now generate the merkle tree
    system.merkle_tree_storage.load_layer(hash, &PathBuf::from(file_path)).await?;

    Ok(())
}
