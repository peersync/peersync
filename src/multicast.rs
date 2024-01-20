use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Error, Result};
use cidr_utils::cidr::Ipv6Cidr;
use futures::{stream::FuturesUnordered, StreamExt};
use log::{debug, info, warn};
use once_cell::sync::OnceCell;
use pnet::datalink::NetworkInterface;
use serde::{Deserialize, Serialize};
use tokio::{net::UdpSocket, sync::oneshot, time::timeout};

use crate::{
    dht::KBucketCommand,
    node::{Key, Node},
    parallel_download::LSCommand,
    system::System,
    util::*,
};

static INTERFACE: OnceCell<NetworkInterface> = OnceCell::new();

#[derive(Serialize, Deserialize)]
enum MulticastMessage {
    Node(Node),
    FindNodeReq(Key), // checks if a peer is local
    FindNodeResp(Key),
    FindPeerReq(Key), // finds peers holding the key of interest, response is MulticastMessage::Node
}

const IPV4_MULTICAST_ADDR: &'static str = "224.0.0.114";
const IPV6_MULTICAST_ADDR: &'static str = "ff12:114:514:1919::810";
const MULTICAST_PORT: u16 = 5679;
const BUFFER_SIZE: usize = 8192;
const BOOTSTRAP_TIMEOUT: u64 = 3;
const LOCAL_PEER_CHECK_TIMEOUT: u64 = 50; // in ms, not s

pub async fn start_multicast_listener(ip: IpAddr, system: Arc<System>) -> Result<()> {
    let socket = SocketAddr::new(ip, MULTICAST_PORT);
    let socket = Arc::new(UdpSocket::bind(socket).await?);
    let multicast_ipv4_addr: Ipv4Addr = IPV4_MULTICAST_ADDR.parse()?;
    let multicast_ipv6_addr: Ipv6Addr = IPV6_MULTICAST_ADDR.parse()?;
    socket.join_multicast_v4(multicast_ipv4_addr, Ipv4Addr::UNSPECIFIED)?;
    socket.join_multicast_v6(&multicast_ipv6_addr, 0)?;

    let mut buf = [0u8; BUFFER_SIZE];

    loop {
        let (len, src) = socket.recv_from(&mut buf).await?;
        let message = match serde_json::from_slice(&buf[..len]) {
            Ok(message) => message,
            Err(e) => {
                warn!("Failed to deserialize multicast message: {e}. ");
                continue;
            }
        };

        let socket = Arc::clone(&socket);
        let system = Arc::clone(&system);
        tokio::spawn(async move {
            if let Err(e) = process_request(message, socket, src, system).await {
                warn!("Failed to process multicast request: {e}. ");
            }
        });
    }
}

async fn process_request(
    message: MulticastMessage,
    socket: Arc<UdpSocket>,
    src: SocketAddr,
    system: Arc<System>,
) -> Result<()> {
    match message {
        MulticastMessage::Node(node) => {
            let response = serde_json::to_string(&MulticastMessage::Node(system.local_node.clone()))?;
            system
                .local_storage_command_tx
                .send(LSCommand::AddLocalNode((node.key.clone()).clone()))?;
            add_node(node, system).await?;
            socket.send_to(response.as_bytes(), src).await?;
        },
        MulticastMessage::FindNodeReq(key) => {
            if key != system.local_node.key {
                return Ok(());
            }

            let response = MulticastMessage::FindNodeResp(key);
            let response = serde_json::to_string(&response)?;
            socket.send_to(response.as_bytes(), src).await?;
        },
        MulticastMessage::FindNodeResp(_) => {},
        MulticastMessage::FindPeerReq(key) => {
            let (tx, rx) = oneshot::channel();
            system.local_storage_command_tx.send(LSCommand::GetReady(key, tx))?;
            let resp = rx.await?;
            if resp.len() == 0 {
                return Ok(());
            }

            let response = serde_json::to_string(&MulticastMessage::Node(system.local_node.clone()))?;
            socket.send_to(response.as_bytes(), src).await?;
        },
    }

    Ok(())
}

pub async fn multicast_bootstrap(interface_name: String, system: Arc<System>) -> Result<()> {
    let interface = get_interface(&interface_name)?;
    INTERFACE.get_or_init(|| interface);

    let message = Arc::new(serde_json::to_string(&MulticastMessage::Node(system.local_node.clone()))?);

    for ip in &INTERFACE.wait().ips {
        let ip = ip.ip();
        let message = Arc::clone(&message);
        let system = Arc::clone(&system);

        tokio::spawn(timeout(
            Duration::from_secs(BOOTSTRAP_TIMEOUT),
            tokio::spawn(async move {
                let mut buf = [0u8; BUFFER_SIZE];

                let (socket, remote) = create_multicast_socket(&ip).await?;
                socket.send_to(message.as_bytes(), remote).await?;

                loop {
                    if let Err(e) = bootstrap_loop_func(&socket, &mut buf, &system).await {
                        warn!("Failed to handle bootstrap response: {e}. ");
                    }
                }

                #[allow(unreachable_code)] // for type annotation only
                Ok::<(), Error>(())
            }),
        ));
    }

    Ok(())
}

async fn bootstrap_loop_func(socket: &UdpSocket, mut buf: &mut [u8], system: &Arc<System>) -> Result<()> {
    let (len, _) = socket.recv_from(&mut buf).await?;
    let message: MulticastMessage = serde_json::from_slice(&buf[..len])?;
    let node = match message {
        MulticastMessage::Node(node) => node,
        _ => return Ok(()),
    };
    system
        .local_storage_command_tx
        .send(LSCommand::AddLocalNode((node.key.clone()).clone()))?;    
    add_node(node, Arc::clone(&system)).await?;
    Ok(())
}

async fn add_node(node: Node, system: Arc<System>) -> Result<()> {
    info!("Got peer {} via multicast. ", node.name);
    let command = KBucketCommand::AddNode(node);
    system.kb_command_tx.send(command)?;
    Ok(())
}

async fn create_multicast_socket(ip: &IpAddr) -> Result<(UdpSocket, SocketAddr)> {
    let (socket, remote) = match ip {
        IpAddr::V4(ipv4) => {
            let socket: SocketAddr = format!("{ipv4}:0").parse()?;
            let socket = UdpSocket::bind(socket).await?;

            let remote: SocketAddr = format!("{IPV4_MULTICAST_ADDR}:{MULTICAST_PORT}").parse()?;
            socket.set_multicast_loop_v4(false)?;

            (socket, remote)
        }
        IpAddr::V6(ipv6) => {
            let link_local = Ipv6Cidr::from_str("fe80::/10")?;
            if link_local.contains(ipv6) {
                return Err(anyhow!("link-local address"));
            }

            let socket: SocketAddr = format!("[{ipv6}]:0").parse()?;
            let socket = UdpSocket::bind(socket).await?;

            let remote: SocketAddr = format!("[{IPV6_MULTICAST_ADDR}]:{MULTICAST_PORT}").parse()?;
            socket.set_multicast_loop_v6(false)?;

            (socket, remote)
        }
    };

    Ok((socket, remote))
}

pub async fn is_peer_local(key: Key, system: Arc<System>) -> bool {
    is_peer_local_impl(key, system).await.unwrap_or(false)
}

async fn is_peer_local_impl(key: Key, system: Arc<System>) -> Result<bool> {
    let (tx, rx) = oneshot::channel();
    system
        .local_storage_command_tx
        .send(LSCommand::IsLocalNode(key.clone(), tx))?;
    let resp = rx.await?;
    if resp {
        debug!("Node {} is local (cache). ", key);
        return Ok(true);
    }

    let message = MulticastMessage::FindNodeReq(key.clone());
    let message = Arc::new(serde_json::to_string(&message)?);
    let key = Arc::new(key);

    const DUR: Duration = Duration::from_millis(LOCAL_PEER_CHECK_TIMEOUT);

    let mut futures = FuturesUnordered::new();
    for ip in &INTERFACE.wait().ips {
        let ip = ip.ip();
        let message = Arc::clone(&message);
        let key = Arc::clone(&key);

        futures.push(tokio::spawn(timeout(
            DUR,
            tokio::spawn(async move {
                let mut buf = [0u8; BUFFER_SIZE];

                let (socket, remote) = create_multicast_socket(&ip).await?;
                socket.send_to(&message.as_bytes(), remote).await?;

                loop {
                    let (len, _) = socket.recv_from(&mut buf).await?;
                    let message: MulticastMessage = serde_json::from_slice(&buf[..len])?;

                    if let MulticastMessage::FindNodeResp(resp_key) = message {
                        if &resp_key != key.as_ref() {
                            continue;
                        }

                        return Ok::<bool, Error>(true);
                    }
                }
            }),
        )));
    }

    while let Some(res) = futures.next().await {
        if let Ok(Ok(Ok(Ok(true)))) = res {
            debug!("Node {} is local (multicast). ", key);
            system
                .local_storage_command_tx
                .send(LSCommand::AddLocalNode((&*key).clone()))?;
            return Ok(true);
        }
    }

    debug!("Node {} is not local. ", key);
    Ok(false)
}

pub async fn find_peer(key: &Key) -> Vec<Node> {
    find_peer_impl(key).await.unwrap_or(vec![])
}

async fn find_peer_impl(key: &Key) -> Result<Vec<Node>> {
    let message = MulticastMessage::FindPeerReq(key.clone());
    let message = Arc::new(serde_json::to_string(&message)?);

    const DUR: Duration = Duration::from_millis(LOCAL_PEER_CHECK_TIMEOUT);

    let peers = Arc::new(Mutex::new(vec![]));
    let mut futures = vec![];
    for ip in &INTERFACE.wait().ips {
        let ip = ip.ip();
        let message = Arc::clone(&message);
        let peers = Arc::clone(&peers);

        futures.push(tokio::spawn(timeout(
            DUR,
            tokio::spawn(async move {
                let mut buf = [0u8; BUFFER_SIZE];

                let (socket, remote) = create_multicast_socket(&ip).await?;
                socket.send_to(&message.as_bytes(), remote).await?;

                loop {
                    let (len, _) = socket.recv_from(&mut buf).await?;
                    let message: MulticastMessage = serde_json::from_slice(&buf[..len])?;

                    if let MulticastMessage::Node(peer) = message {
                        peers.lock().map_err(|_| anyhow!("failed to acquire lock"))?.push(peer);
                    }
                }

                #[allow(unreachable_code)] // for type annotation only
                Ok::<(), Error>(())
            }),
        )));
    }

    for future in futures {
        if let Err(e) = future.await??? {
            warn!("Failed to get peers from multicast: {e}. ");
        }
    }

    let peers = Arc::try_unwrap(peers).map_err(|_| anyhow!("cannot take ownership of peer list"))?;
    let peers = peers.into_inner().map_err(|_| anyhow!("failed to remove lock"))?;

    Ok(peers)
}
