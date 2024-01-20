use crate::node::Key;

pub struct DockerManagedImage {
    pub name: String,
    pub tag: String,
    pub layers: Vec<Key>,
}

pub struct DockerManagedImageList {
    pub images: Vec<DockerManagedImage>,
}
