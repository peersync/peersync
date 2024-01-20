use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::{anyhow, Result};
use pnet::datalink::{self, NetworkInterface};
use rand::seq::SliceRandom;

use crate::node::Node;

pub use crate::parallel_download::util::*;

pub fn addr_from_node(node: &Node) -> Option<SocketAddr> {
    node.addr.choose(&mut rand::thread_rng()).cloned()
}

pub fn string_to_socket(s: &str) -> Result<SocketAddr> {
    if let Ok(addr) = s.parse::<SocketAddr>() {
        return Ok(addr);
    }

    let host = s.to_socket_addrs()?.next().ok_or(anyhow!("cannot resolve hostname {s}"))?;

    Ok(host)
}

pub fn proto_from_node(node: &Node) -> &'static str {
    if node.tls {
        "https"
    } else {
        "http"
    }
}

pub fn get_interface(interface_name: &str) -> Result<NetworkInterface> {
    let interface = datalink::interfaces()
        .iter()
        .filter(|i| i.name == interface_name)
        .next()
        .cloned();

    let interface = match interface {
        Some(interface) => interface,
        None => return Err(anyhow!("specified multicast interface not found")),
    };

    Ok(interface)
}

pub fn normalize(src: Vec<f64>) -> Vec<f64> {
    if src.is_empty() {
        return src;
    }

    let min = src.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = src.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    if max - min == 0.0 {
        return src.iter().map(|_| 0.0).collect();
    }

    src.iter().map(|&x| (x - min) / (max - min)).collect()
}
