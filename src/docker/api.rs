use std::collections::HashSet;

use anyhow::{anyhow, Result};
use hyper::{body, Body, Client, Method, Response, Request, StatusCode};
use hyperlocal::{UnixClientExt, Uri as LocalUri};
use log::{error, info, warn};
use serde::Deserialize;

use crate::{
    docker::managed_image::{DockerManagedImage, DockerManagedImageList},
    node::Key,
};

async fn make_request(socket: &str, method: Method, uri: &str) -> Result<Response<Body>> {
    let uri: LocalUri = LocalUri::new(socket, &uri);
    let client = Client::unix();

    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())?;

    let response = client.request(request).await?;

    Ok(response)
}

async fn get_api_version(socket: &str) -> Result<(String, Option<String>)> {
    let response = make_request(socket, Method::GET, "localhost/version").await?;

    if response.status() != StatusCode::OK {
        return Err(anyhow!("Docker API returned non-OK status code: {}", response.status().as_u16()));
    }

    let response = body::to_bytes(response).await?;

    #[derive(Deserialize)]
    struct Response {
        #[serde(rename = "ApiVersion")]
        api_version: String,
        #[serde(rename = "MinApiVersion")]
        min_api_version: Option<String>,
    }

    let response: Response = serde_json::from_slice(&response)?;

    Ok((response.api_version, response.min_api_version))
}

pub async fn show_api_version(socket: &str) {
    let api_ver = match get_api_version(socket).await {
        Ok(x) => x,
        Err(e) => {
            error!("Failed to get Docker API version: {e}. ");
            return;
        },
    };

    info!("Current API version: {}. ", api_ver.0);
    match api_ver.1 {
        Some(x) => info!("Minimum API version: {}. ", x),
        None => info!("Docker API didn't specify minimum API version. "),
    }
}

pub async fn get_managed_images(socket: &str) -> Result<DockerManagedImageList> {
    let image_list = {    
        let response = make_request(socket, Method::GET, "localhost/images/json").await?;
    
        if response.status() != StatusCode::OK {
            return Err(anyhow!("Docker API returned non-OK status code: {}", response.status().as_u16()));
        }

        let response = body::to_bytes(response).await?;

        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "RepoTags")]
            repo_tags: Vec<String>,
        }

        let response: Vec<Response> = serde_json::from_slice(&response)?;
        let mut list = vec![];
        for r in response {
            list.extend(r.repo_tags);
        }

        let list: Vec<_> = list
            .into_iter()
            .map(|name| {
                let parts: Vec<_> = name.split(":").collect();
                let name = parts[0].to_string();
                let tag = parts[1].to_string();
                (name, tag)
            })
            .collect();

        list
    };

    let mut images = vec![];
    for (name, tag) in image_list {
        let uri = format!("localhost/images/{}/json", name);
        let response = make_request(socket, Method::GET, &uri).await?;
    
        if response.status() != StatusCode::OK {
            continue;
        }

        let response = body::to_bytes(response).await?;

        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "RootFS")]
            root_fs: RootFS,
        }

        #[derive(Deserialize)]
        struct RootFS {
            #[serde(rename = "Layers")]
            layers: Vec<String>,
        }

        let response: Response = serde_json::from_slice(&response)?;
        let layers = response.root_fs.layers;
        let layers: Vec<_> = layers
            .into_iter()
            .map(|layer| {
                let hash = layer.split(":").last().unwrap();
                let hash = Key::from_str(hash).unwrap();
                hash
            })
            .collect();

        let image_info = DockerManagedImage {
            name,
            tag,
            layers,
        };

        images.push(image_info);
    }

    let list = DockerManagedImageList {
        images,
    };

    Ok(list)
}

pub async fn get_used_layers(socket: &str) -> Result<Vec<Key>> {
    let images = get_managed_images(socket).await?;

    let used_images = {
        let response = make_request(socket, Method::GET, "localhost/containers/json").await?;
    
        if response.status() != StatusCode::OK {
            warn!("Failed to get list of active images. ");
        }

        let response = body::to_bytes(response).await?;

        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "Image")]
            image: String,
        }
        let response: Vec<Response> = serde_json::from_slice(&response)?;

        let used_images: HashSet<_> = response
            .into_iter()
            .map(|r| r.image)
            .collect();

        used_images
    };

    let used_layers = {
        let mut used_layers = HashSet::new();
        for image in images.images {
            let name = image.name;
            if used_images.contains(&name) {
                used_layers.extend(image.layers);
            }
        }

        used_layers
    };

    let result: Vec<_> = used_layers.into_iter().collect();

    Ok(result)
}
