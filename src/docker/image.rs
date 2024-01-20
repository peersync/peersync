use std::{
    collections::HashMap,
    fs::metadata,
    io::SeekFrom,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{anyhow, Result};
use axum::{
    body::StreamBody,
    extract::{ConnectInfo, Extension, Path as ExtractPath},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use bytes::Bytes;
use futures::Stream;
use log::{debug, warn};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    fs::File,
    sync::{mpsc::{self, UnboundedReceiver, UnboundedSender}, oneshot},
};

use crate::{
    node::Key,
    parallel_download::{download, get_block_size, LSCommand},
    system::System,
    util::*,
};

pub struct ImageStream {
    rx: UnboundedReceiver<Result<Bytes>>,
}

impl Stream for ImageStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = match self.rx.poll_recv(cx) {
            Poll::Ready(Some(res)) => res,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        if let Err(e) = &res {
            warn!("Failed to transfer block: {e}. ");
        }

        Poll::Ready(Some(res))
    }
}

impl ImageStream {
    fn new(rx: UnboundedReceiver<Result<Bytes>>) -> Self {
        Self {
            rx,
        }
    }
}

pub async fn image_handler(
    ExtractPath(params): ExtractPath<HashMap<String, String>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Extension(system): Extension<Arc<System>>,
) -> impl IntoResponse {
    let name = match params.get("name") {
        Some(name) => name,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    let digest = match params.get("digest") {
        Some(digest) => digest,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    let name = match params.get("repo") {
        Some(repo) => format!("{repo}/{name}"),
        None => name.to_owned(),
    };

    debug!("Got Docker registry download request for layer {digest} of image {name} from {addr}. ");

    let (headers, resp) = match image_impl(name, digest, headers, system).await {
        Ok((headers, resp)) => (headers, resp),
        Err(e) => {
            let message = format!("Failed to process request: {e}. ");
            return (StatusCode::BAD_REQUEST, message).into_response();
        },
    };

    let stream = ImageStream::new(resp);
    let stream = StreamBody::new(stream);

    (StatusCode::OK, headers, stream).into_response()
}

async fn image_impl(
    name: String,
    digest: &str,
    headers: HeaderMap,
    system: Arc<System>,
) -> Result<(HeaderMap, UnboundedReceiver<Result<Bytes>>)> {
    let requested_block = match headers.get("X_REQUEST_BLOCK") {
        Some(requested_block) => {
            let requested_block = requested_block.to_str()?;
            let requested_block: usize = requested_block.parse()?;
            Some(requested_block)
        },
        None => None,
    };

    let hash = Key::from_str(&digest["sha256:".len()..])?;

    let file_path = format!("{}/{hash}", system.cache_directory);

    let (data_tx, data_rx) = mpsc::unbounded_channel();

    let size = {
        let (tx, rx) = oneshot::channel();
        system.local_storage_command_tx.send(LSCommand::GetSize(hash.clone(), tx))?;
        rx.await?.ok_or(anyhow!("size for {hash} not found"))?
    };

    // start and end are actual index of byte!!!
    let (start, mut end) = match requested_block {
        Some(requested_block) => block_to_byte(requested_block, size),
        None => (0, size - 1),
    };

    // todo: if two downloads start???
    if Path::new(&file_path).exists() {
        let md = metadata(&file_path)?;
        if end >= md.len() as usize {
            end = md.len() as usize - 1;
        }

        let mut headers = HeaderMap::new();
        headers.append(header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
        headers.append(header::CONTENT_LENGTH, HeaderValue::from(end - start + 1));

        tokio::spawn(async move {
            if let Err(e) = read_file(&file_path, start, end, data_tx, size).await {
                warn!("Image reader failed: {e}. ");
            }
        });

        return Ok((headers, data_rx));
    }

    if requested_block.is_some() {
        return Err(anyhow!("requested resource not cached"));
    }

    // now, the request comes from Docker directly and we don't have cache
    // time to relay the request
    let (resp_code, resp_headers, resp_data) = {
        let (tx, rx) = oneshot::channel();
        let name = name.to_owned();
        let system = Arc::clone(&system);
        tokio::spawn(async move {
            if let Err(e) = download(name, hash, tx, data_tx, system).await {
                warn!("Failed to download: {e}. ");
            }
        });
        rx.await?
    };

    if resp_code != 200 {
        let message = std::str::from_utf8(&resp_data)?.to_owned();
        return Err(anyhow!(message));
    }

    Ok((resp_headers, data_rx))
}

async fn read_file(
    path: &str,
    start: usize,
    end: usize,
    tx: UnboundedSender<Result<Bytes>>,
    size: usize,
) -> Result<()> {
    let target_len = end - start + 1;

    let mut file = match File::open(path).await {
        Ok(file) => file,
        Err(e) => {
            tx.send(Err(e.into()))?;
            return Err(anyhow!("I/O failure"));
        },
    };

    if let Err(e) = file.seek(SeekFrom::Start(start as u64)).await {
        tx.send(Err(e.into()))?;
        return Err(anyhow!("I/O failure"));
    }

    let block_size = get_block_size(size);

    let mut buffer = vec![0u8; block_size];
    let mut total_read = 0;
    loop {
        let len = match file.read(&mut buffer).await {
            Ok(len) => len,
            Err(e) => {
                tx.send(Err(e.into()))?;
                return Err(anyhow!("I/O failure"));
            },
        };

        if total_read + len >= target_len {
            let remaining = target_len - total_read;
            let data = &buffer[..remaining];
            let data = Bytes::from_iter(data.into_iter().map(|byte| *byte));
            tx.send(Ok(data))?;
            break;
        }

        let data = &buffer[..len];
        let data = Bytes::from_iter(data.into_iter().map(|byte| *byte));
        tx.send(Ok(data))?;
        total_read += len;
    }

    Ok(())
}
