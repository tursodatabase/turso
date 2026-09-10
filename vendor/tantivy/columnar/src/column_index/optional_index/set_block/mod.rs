mod dense;
mod sparse;

pub use dense::{DENSE_BLOCK_NUM_BYTES, DenseBlock, DenseBlockCodec};
pub use sparse::{SparseBlock, SparseBlockCodec};

#[cfg(test)]
mod tests;
