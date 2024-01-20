use std::{
    collections::HashMap,
    ffi::CString,
    io::Error as IOError,
    mem::zeroed,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Result};
use chrono::Utc;
use libc::{statvfs, c_char};
use log::{debug, info, warn};
use tokio::{fs::remove_file, time::sleep};

use crate::{
    docker::get_used_layers,
    multicast::find_peer,
    node::Key,
    parallel_download::LSCommand,
    system::System,
};

const CLEANER_INTERVAL: u64 = 60; // cleaner runs every 60 seconds
const CLEANER_THRESHOLD: u64 = 1 * 1024 * 1024 * 1024; // 10 GiB

#[derive(Clone)]
struct ImageUsage {
    // the last time when an image was used
    usage: HashMap<Key, i64>,
}

pub struct ImageCleaner {
    interval: u64,
    socket: String,
    cache_dir: String,
    usage: Mutex<ImageUsage>,
    system: Arc<System>,
}

impl Default for ImageUsage {
    fn default() -> Self {
        Self {
            usage: HashMap::new(),
        }
    }
}

impl ImageCleaner {
    pub fn new(socket: String, cache_dir: String, system: Arc<System>) -> Self {
        Self {
            interval: CLEANER_INTERVAL,
            socket,
            cache_dir,
            usage: Mutex::new(ImageUsage::default()),
            system,
        }
    }

    pub async fn start(self) {
        loop {
            if let Err(e) = self.exec_impl().await {
                warn!("Failed to clean images: {e}");
            }

            sleep(Duration::from_secs(self.interval)).await;
        }
    }

    async fn exec_impl(&self) -> Result<()> {
        let active_layers = get_used_layers(&self.socket).await?;
        let time = Utc::now().timestamp();
        {
            let mut usage = self.usage.lock().map_err(|_| anyhow!("failed to acquire lock"))?;
            for layer in active_layers {
                usage.usage.insert(layer, time);
            }
        }

        let path = "/var/lib/docker";
        // in bytes
        let (total, available) = get_filesystem_info(path)?;
        let ratio = available as f64 / total as f64;

        if available > CLEANER_THRESHOLD || ratio > 0.1 {
            return Ok(());
        }

        info!("Low disk space, cleaning images. Remaining disk space: {available} byte(s). ");

        // remove the oldest image
        let image_to_clean = {
            let usage = self.usage.lock().map_err(|_| anyhow!("failed to acquire lock"))?.clone();
            let image = decide_layer_to_remove(usage).await?;
            image
        };

        // image removed from cache, now do the actual cleaning
        remove_file(format!("{}/{image_to_clean}", self.cache_dir)).await?;

        // so, we no longer have it
        self.usage.lock().map_err(|_| anyhow!("failed to acquire lock"))?.usage.remove(&image_to_clean);
        self.system.local_storage_command_tx.send(LSCommand::RemoveReady(image_to_clean))?;

        Ok(())
    }
}

fn get_filesystem_info<P: AsRef<Path>>(path: P) -> Result<(u64, u64)> {
    let path = path.as_ref().to_str().ok_or(anyhow!("invalid path"))?;
    let c_path = CString::new(path)?;
    
    let stat = unsafe {
        let mut stat: statvfs = zeroed();
        let result = statvfs(c_path.as_ptr() as *const c_char, &mut stat);
        if result != 0 {
            return Err(IOError::last_os_error().into());
        }
        stat
    };
    
    let total_size = stat.f_blocks * stat.f_frsize;
    let available_size = stat.f_bavail * stat.f_frsize;
    Ok((total_size as u64, available_size as u64))
}

async fn decide_layer_to_remove(usage: ImageUsage) -> Result<Key> {
    let usage: Vec<_> = usage.usage
        .into_iter()
        .map(|u| (u.0, u.1))
        .collect();

    // age
    let time_now = Utc::now().timestamp();
    let mut scores: HashMap<_, _> = usage
        .iter()
        .map(|(key, last_use)| (key, (time_now - last_use) as f64))
        .collect();

    // generelization
    let max_score = match scores.values().max_by(|a, b| a.partial_cmp(b).unwrap()) {
        Some(score) => *score,
        None => return Err(anyhow!("no image to remove")),
    };

    for (_, score) in scores.iter_mut() {
        *score /= max_score;
    }

    // local availablity
    let mut handles = vec![];
    for (key, _) in &usage {
        let key = key.clone();
        let handle = tokio::spawn(async move {
            (find_peer(&key).await, key)
        });
        handles.push(handle);
    }

    let mut availablity = HashMap::new();
    for handle in handles.into_iter() {
        let (resp, key) = handle.await?;
        availablity.insert(key, resp.len());
    }

    let max_availablity = match availablity.values().max_by(|a, b| a.cmp(b)) {
        Some(score) => *score as f64,
        None => return Err(anyhow!("no image to remove")),
    };

    let availablity: HashMap<_, _> = availablity
        .into_iter()
        .map(|(key, a) | (key, a as f64 / max_availablity))
        .collect();

    for (key, score) in scores.iter_mut() {
        let a = availablity.get(key).ok_or(anyhow!("no image to remove"))?;
        *score = (*score + *a) / 2.0;
    }

    // find the layer with max score
    let entry = scores
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| {
            let a = a.1;
            let b = b.1;
            a.partial_cmp(&b).unwrap()
        })
        .map(|(idx, (_, score))| (idx, score));

    let (idx, max_score) = entry.ok_or(anyhow!("no image to remove"))?;
    let layer = usage.get(idx).ok_or(anyhow!("no image to remove"))?.0.clone();

    info!("Removing layer {layer}. ");
    debug!("Layer removal score: {max_score}. ");

    Ok(layer)
}
