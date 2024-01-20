use std::{net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr}, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use itertools::Itertools;
use log::{debug, info, warn};
use reqwest::Client;
use tokio::{sync::mpsc::{self, unbounded_channel, UnboundedReceiver}, time::sleep};

use crate::{
    cleaner::ImageCleaner,
    config::{Bootstrap, Config},
    dht::KBucket,
    docker::{get_managed_images, init_cache, show_api_version},
    multicast::{multicast_bootstrap, start_multicast_listener},
    node::{Key, Node},
    parallel_download::{LSCommand, LocalStorage},
    peer::PeerManager,
    server::start_server,
    system::System,
    tracker::{Tracker, TrackerElectionManager, TrackerList, TrackerListCommand},
    util::*,
};

const IPV4_WILDCARD: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
const IPV6_WILDCARD: IpAddr = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0));
const CACHE_DIR: &str = "/var/lib/docker";

pub struct App {
    node: Arc<Node>,
    kb: KBucket,
    peer_manager: PeerManager,
    tls: bool,
    certificate: Option<String>,
    privkey: Option<String>,
    multicast_interface: String,
    bootstrap: Vec<Bootstrap>,
    docker_socket: String,
    tracker_list_command_rx: UnboundedReceiver<TrackerListCommand>,
    tracker_election_manager: TrackerElectionManager,
    system: Arc<System>,
    local_storage_command_rx: UnboundedReceiver<LSCommand>,
    image_cleaner: ImageCleaner,
}

impl App {
    pub fn new(config: Config) -> Result<Self> {
        let node = {
            let mut id = config.node_name.clone();
            id.push_str(&format!("{:?}", config.listen));

            let key = Key::new(id.as_bytes());
            info!("Local node is {} with key {key}. ", config.node_name);

            let listen = config.listen.unwrap_or(vec![]);
            for addr in &listen {
                info!("Listening on {addr}. ");
            }

            let external_listen = {
                let interface = get_interface(&config.interface)?;
                let mut ipv4 = vec![];
                let mut ipv6 = vec![];
                for addr in interface.ips {
                    let ip = addr.ip();
                    match ip {
                        IpAddr::V4(ipv4_addr) => ipv4.push(ipv4_addr),
                        IpAddr::V6(ipv6_addr) => {
                            if ipv6_addr.is_unicast() && !ipv6_addr.is_unicast_link_local() {
                                ipv6.push(ipv6_addr);
                            }
                        },
                    }
                }
    
                let mut external_listen = vec![];
                for addr in &listen {
                    let ip = addr.ip();
                    let port = addr.port();
                    if ip == IPV4_WILDCARD {
                        for ip in &ipv4 {
                            external_listen.push(SocketAddr::new(IpAddr::V4(*ip), port));
                        }
                    }
    
                    if ip == IPV6_WILDCARD {
                        for ip in &ipv6 {
                            external_listen.push(SocketAddr::new(IpAddr::V6(*ip), port));
                        }

                        for ip in &ipv4 {
                            external_listen.push(SocketAddr::new(IpAddr::V4(*ip), port));
                        }
                    }
                }
    
                external_listen.retain(|addr| addr.ip() != IPV4_WILDCARD && addr.ip() != IPV6_WILDCARD);
                external_listen.into_iter().unique().collect::<Vec<_>>()
            };

            for addr in &listen {
                info!("Listening on {addr}. ");
            }

            Arc::new(Node::new(config.node_name, key, external_listen, config.tls))
        };

        let (kb_command_tx, kb_command_rx) = mpsc::unbounded_channel();
        let (peer_command_tx, peer_command_rx) = mpsc::unbounded_channel();
        let (tracker_list_command_tx, tracker_list_command_rx) = mpsc::unbounded_channel();

        let bootstrap = config.bootstrap.unwrap_or(vec![]);

        let tracker = Tracker::default();

        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout))
            .build()?;

        let (ls_tx, ls_rx) = unbounded_channel();

        let system = Arc::new(System::new(
            node.as_ref().clone(),
            kb_command_tx,
            peer_command_tx,
            tracker_list_command_tx,
            ls_tx,
            tracker,
            client,
            config.docker_registry,
            config.cache_directory,
            config.record,
            config.record_path,
        ));

        let tracker_election_manager = TrackerElectionManager::new(Arc::clone(&system));

        let image_cleaner = ImageCleaner::new(
            config.docker_socket.clone(),
            String::from(CACHE_DIR),
            Arc::clone(&system),
        );

        Ok(Self {
            node: Arc::clone(&node),
            kb: KBucket::new(config.k_param, kb_command_rx, Arc::clone(&system)),
            peer_manager: PeerManager::new(peer_command_rx),
            tls: config.tls,
            certificate: config.certificate,
            privkey: config.privkey,
            multicast_interface: config.interface,
            bootstrap,
            docker_socket: config.docker_socket,
            tracker_list_command_rx,
            tracker_election_manager,
            system,
            local_storage_command_rx: ls_rx,
            image_cleaner,
        })
    }

    pub async fn start(self) -> Result<()> {
        let system_tracker = Arc::clone(&self.system);
        tokio::spawn(async move {
            system_tracker.tracker.start_cleaner().await;
        });

        let election_manager = Arc::new(self.tracker_election_manager);
        let tracker_election_manager = tokio::spawn(Arc::clone(&election_manager).start());

        let tracker_list = tokio::spawn(TrackerList::start(self.tracker_list_command_rx));

        show_api_version(&self.docker_socket).await;
        init_cache(Arc::clone(&self.system)).await?;

        let docker_socket = self.docker_socket.clone();
        const DOCKER_IMAGE_POLL_INTERVAL: Duration = Duration::from_secs(10);
        tokio::spawn(async move {
            loop {
                let _images = match get_managed_images(&docker_socket).await {
                    Ok(images) => images,
                    Err(e) => {
                        warn!("Failed to get images managed by Docker Engine: {e}. ");
                        sleep(DOCKER_IMAGE_POLL_INTERVAL).await;
                        continue;
                    },
                };

                sleep(DOCKER_IMAGE_POLL_INTERVAL).await;
            }
        });

        let k_bucket_manager = tokio::spawn(self.kb.start_manager());

        let peer_manager = tokio::spawn(self.peer_manager.start_manager());

        let multicast_listen_on = "::".parse::<IpAddr>()?;
        let multicast_listener =
            tokio::spawn(start_multicast_listener(multicast_listen_on, Arc::clone(&self.system)));

        let multicast_bootstrap =
            tokio::spawn(multicast_bootstrap(
                self.multicast_interface,
                Arc::clone(&self.system),
            ));

        let listen = self.node.addr.clone();
        let system_server = Arc::clone(&self.system);
        let server = tokio::spawn(async move {
            start_server(
                &listen,
                self.tls,
                self.certificate,
                self.privkey,
                system_server,
                election_manager,
            ).await
        });

        let local_node = Arc::clone(&self.node);
        if let Err(e) = local_node
            .bootstrap(&self.bootstrap, Arc::clone(&self.system))
            .await
        {
            warn!("Failed to bootstrap: {e}. ");
        }

        debug!("DHT initialization complete. ");

        let local_status = LocalStorage::start(self.local_storage_command_rx);

        let image_cleaner = tokio::spawn(self.image_cleaner.start());

        tokio::select! {
            Err(e) = k_bucket_manager => Err(anyhow!("k bucket manager exited unexpectedly: {e}. "))?,
            Err(e) = peer_manager => Err(anyhow!("peer manager exited unexpectedly: {e}. "))?,
            Err(e) = multicast_listener => Err(anyhow!("multicast listener exited unexpectedly: {e}. "))?,
            Err(e) = multicast_bootstrap => Err(anyhow!("multicast bootstrap exited unexpectedly: {e}. "))?,
            Err(e) = server => Err(anyhow!("server exited unexpectedly: {e}. "))?,
            Err(e) = tracker_election_manager => Err(anyhow!("tracker election manager exited unexpectedly: {e}. "))?,
            Err(e) = tracker_list => Err(anyhow!("tracker list exited unexpectedly: {e}. "))?,
            Err(e) = local_status => Err(anyhow!("local storage manager exited unexpectedly: {e}. "))?,
            Err(e) = image_cleaner => Err(anyhow!("image cleaner exited unexpectedly: {e}. "))?,
        }

        Err(anyhow!("server failed"))?
    }
}
