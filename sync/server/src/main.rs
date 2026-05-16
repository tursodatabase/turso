#![allow(clippy::arc_with_non_send_sync)]

mod bootstrap;
mod database;
mod sync_server;

use crate::{bootstrap::ServerApp, sync_server::TursoSyncServer};

#[cfg(all(feature = "mimalloc", not(target_family = "wasm"), not(miri)))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> anyhow::Result<()> {
    let app = ServerApp::new()?;

    TursoSyncServer::new(
        app.opts.sync_server_address.clone(),
        app.database_provider,
        app.interrupt_count,
    )
    .run()
}
