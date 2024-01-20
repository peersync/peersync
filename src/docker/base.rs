use std::net::SocketAddr;

use axum::{
    extract::ConnectInfo,
    http::{header, StatusCode},
    response::IntoResponse,
};
use log::debug;

pub async fn base_handler(ConnectInfo(addr): ConnectInfo<SocketAddr>) -> impl IntoResponse {
    debug!("Got Docker registry proxy request from {addr}. ");
    let headers = [
        (header::CONTENT_TYPE, "application/json"),
        (header::CONNECTION, "close"),
    ];
    (StatusCode::OK, headers, "{}").into_response()
}
