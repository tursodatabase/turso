use std::{fmt::Debug, fs::read_to_string, io::Result, path::Path};

use tracing::{debug, trace};

// Starship function
/// Return the string contents of a file
pub fn read_file<P: AsRef<Path> + Debug>(file_name: P) -> Result<String> {
    trace!("Trying to read from {:?}", file_name);

    let result = read_to_string(file_name);

    if result.is_err() {
        debug!("Error reading file: {:?}", result);
    } else {
        trace!("File read successfully");
    };

    result
}
