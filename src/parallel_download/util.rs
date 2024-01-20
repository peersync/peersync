use super::{get_block_size, local_storage::Range};

pub fn byte_to_block(start: usize, end: usize, total: usize) -> Range {
    let block_size = get_block_size(total);
    Range(start / block_size, end / block_size)
}

pub fn block_to_byte(block: usize, total: usize) -> (usize, usize) {
    let block_size = get_block_size(total);
    (block * block_size, (block + 1) * block_size - 1)
}
