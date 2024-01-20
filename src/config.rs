use std::{net::SocketAddr, fs};

use anyhow::{anyhow, Result};
use futures::{executor::block_on, TryStreamExt};
use log::{debug, info, warn, error};
use pnet::datalink;
use rtnetlink::{new_connection, IpVersion};
use serde::Deserialize;

const K_PARAM_DEFAULT: usize = 8;
const TIMEOUT_DEFAULT: u64 = 60;
const DOCKER_SOCKET_DEFAULT: &str = "/var/run/docker.sock";
const DOCKER_REGISTRY_DEFAULT: &str = "https://registry.hub.docker.com";
const TLS_DEFAULT: bool = false;
const RECORD_DEFAULT: bool = false;
const RECORD_PATH_DEFAULT: &str = "/tmp/record.txt";

#[derive(Clone, Deserialize)]
pub struct Bootstrap {
    pub node_name: String,
    pub addr: String,

    #[serde(default = "tls_default")]
    pub tls: bool,
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub node_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen: Option<Vec<SocketAddr>>,

    #[serde(default = "tls_default")]
    pub tls: bool,

    #[serde(default = "k_param_default")]
    pub k_param: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<Vec<Bootstrap>>,

    #[serde(default = "timeout_default")]
    pub timeout: u64,

    #[serde(default = "docker_socket_default")]
    pub docker_socket: String,

    #[serde(default = "docker_registry_default")]
    pub docker_registry: String,

    #[serde(default = "cache_directory_default")]
    pub cache_directory: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub privkey: Option<String>,

    #[serde(default = "interface_default")]
    pub interface: String,

    #[serde(default = "record_default")]
    pub record: bool,

    #[serde(default = "record_path_default")]
    pub record_path: String,
}

impl Config {
    pub fn new(config_path: &str) -> Result<Self> {
        info!("Using configuration file {config_path}. ");

        let config = fs::read_to_string(config_path)?;
        let config = config.trim();
        let config: Self = serde_yaml::from_str(config)?;

        if !config.tls && config.certificate.is_some() {
            warn!("TLS is disabled but config.certificate is set, ignoring. ");
        }

        if !config.tls && config.privkey.is_some() {
            warn!("TLS is disabled but config.privkey is set, ignoring. ");
        }

        if config.tls && config.certificate.is_none() {
            error!("TLS is enabled but config.certificate is not set. ");
            return Err(anyhow!("invalid configuration"));
        }

        if config.tls && config.privkey.is_none() {
            error!("TLS is enabled but config.privkey is not set. ");
            return Err(anyhow!("invalid configuration"));
        }

        Ok(config)
    }
}

fn tls_default() -> bool {
    debug!("Defaulting config.tls to {TLS_DEFAULT}. ");
    TLS_DEFAULT
}

fn record_default() -> bool {
    debug!("Defaulting config.record to {RECORD_DEFAULT}. ");
    RECORD_DEFAULT
}

fn record_path_default() -> String {
    debug!("Defaulting config.record_path to {RECORD_PATH_DEFAULT}. ");
    String::from(RECORD_PATH_DEFAULT)
}

fn k_param_default() -> usize {
    debug!("Defaulting config.k_param to {K_PARAM_DEFAULT}. ");
    K_PARAM_DEFAULT
}

fn timeout_default() -> u64 {
    debug!("Defaulting config.timeout to {TIMEOUT_DEFAULT}. ");
    TIMEOUT_DEFAULT
}

fn docker_socket_default() -> String {
    debug!("Defaulting config.docker_socket to {DOCKER_SOCKET_DEFAULT}. ");
    String::from(DOCKER_SOCKET_DEFAULT)
}

fn docker_registry_default() -> String {
    debug!("Defaulting config.docker_registry to {DOCKER_REGISTRY_DEFAULT}. ");
    String::from(DOCKER_REGISTRY_DEFAULT)
}

fn cache_directory_default() -> String {
    let directory = format!("/var/{}", env!("CARGO_PKG_NAME"));
    debug!("Defaulting config.cache_directory to {directory}. ");
    directory
}

fn interface_default() -> String {
    let interface = match block_on(interface_default_impl()) {
        Ok(interface) => interface,
        Err(e) => {
            warn!("Failed to determine primary interface: {e}. Using \"lo\". ");
            warn!("You should manually configure an interface in this case! ");
            String::from("lo")
        },
    };

    debug!("Defaulting config.interface to {interface}. ");
    interface
}

async fn interface_default_impl() -> Result<String> {
    let (connection, handle, _) = new_connection()?;
    let conn = tokio::spawn(connection);

    let interface = {
        let mut routes = handle.route().get(IpVersion::V4).execute();

        loop {
            let route = match routes.try_next().await? {
                Some(route) => route,
                None => return Err(anyhow!("no default route found")),
            };

            let index = match route.output_interface() {
                Some(index) => index,
                None => continue,
            };

            if route.header.destination_prefix_length == 0 {
                conn.abort();
                break index;
            }
        }
    };

    let interface = datalink::interfaces()
        .into_iter()
        .filter(|i| i.index == interface)
        .next()
        .ok_or(anyhow!("invalid interface"))?;

    Ok(interface.name)
}
