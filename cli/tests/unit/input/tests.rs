use super::{get_io, DbLocation};

#[test]
fn experimental_win_iocp_backend_is_available_for_path_databases() {
    let io = get_io(DbLocation::Path, "experimental_win_iocp")
        .expect("windows cli should construct the experimental_win_iocp backend");
    drop(io);
}
