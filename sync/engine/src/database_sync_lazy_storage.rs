use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use turso_core::{
    storage::sqlite3_ondisk::{self, PageContent},
    Buffer, Completion, DatabaseStorage, File, LimboError,
};

use crate::{
    database_sync_engine_io::SyncEngineIo,
    database_sync_operations::{pull_pages_v1, SyncEngineIoStats, PAGE_SIZE},
    errors,
    types::{Coro, PartialSyncOpts},
};

/// [PageStates] holds information about active operations with pages in the [LazyDatabaseStorage]
struct PageStates {
    /// HashMap from page number (zero-based) to the [PageInfo]
    pages: HashMap<usize, PageInfo>,
}

/// [PageInfo] holds information about page state with some active operation
///
/// Page loading process implemented with deduplication logic,
/// so that if some request want to load page which is already Loading,
/// then it just "subscribe" to the result and wait for anothe operation to complete.
struct PageInfo {
    /// current active operation (operations are mutually exclusive)
    operation: PageOperation,
    /// result of the [PageOperation::Load] operation
    load_result: Option<Result<Vec<u8>, errors::Error>>,
    /// amount of "subscribers" who waits result of the [PageOperation::Load] operation
    load_waits: usize,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum PageOperation {
    /// Load operation triggered during read from the db file
    Load,
    /// Write operation triggered during write (checkpoint) to the db file
    Write,
}

enum PageLoadAction {
    /// Caller must load the page
    Load,
    /// Caller must wait for the load operation result
    Wait,
}

impl PageStates {
    pub fn new() -> Self {
        Self {
            pages: HashMap::new(),
        }
    }
    /// try to start Write opreation for the page
    /// returns Err(...) if another operation already started (Load or Write)
    pub fn write_start(&mut self, page_no: usize) -> Result<(), errors::Error> {
        if self.pages.contains_key(&page_no) {
            return Err(errors::Error::DatabaseSyncEngineError(format!(
                "unable to get write lock: page {page_no} already buys"
            )));
        }
        let info = PageInfo {
            operation: PageOperation::Write,
            load_result: None,
            load_waits: 0,
        };
        self.pages.insert(page_no, info);
        Ok(())
    }
    /// finish Write operation previously started with [Self::write_start]
    pub fn write_end(&mut self, page_no: usize) {
        let Some(info) = self.pages.remove(&page_no) else {
            panic!("page state must be set before write_end");
        };
        assert_eq!(info.operation, PageOperation::Write);
        assert_eq!(info.load_waits, 0);
        assert!(info.load_result.is_none());
    }
    /// try to start Load operation for the page
    /// returns Err(...) if Write operation is on-going
    /// returns Ok(PageLoadAction::Load) if this page wasn't active before and caller must start load process
    /// returns Ok(PageLoadAction::Wait) if this page already loading and caller just needs to wait for result
    pub fn load_start(&mut self, page_no: usize) -> Result<PageLoadAction, errors::Error> {
        match self.pages.get_mut(&page_no) {
            Some(PageInfo {
                operation: PageOperation::Write,
                ..
            }) => Err(errors::Error::DatabaseSyncEngineError(format!(
                "unable to get load lock: page {page_no} already buys"
            ))),
            Some(PageInfo {
                operation: PageOperation::Load,
                load_waits: ref mut subscribers,
                ..
            }) => {
                *subscribers += 1;
                Ok(PageLoadAction::Wait)
            }
            None => {
                let info = PageInfo {
                    operation: PageOperation::Load,
                    load_result: None,
                    load_waits: 0,
                };
                self.pages.insert(page_no, info);
                Ok(PageLoadAction::Load)
            }
        }
    }
    /// finish Load operation with result for the page previously started with [Self::load_start]
    /// caller must use this method only if [Self::load_start] returned Ok(PageLoadAction::Load)
    pub fn load_end(&mut self, page_no: usize, result: Result<Vec<u8>, errors::Error>) {
        let Some(info) = self.pages.get_mut(&page_no) else {
            panic!("page state must be set before load_end");
        };
        assert_eq!(info.operation, PageOperation::Load);
        if info.load_waits > 0 {
            info.load_result = Some(result);
        } else {
            let _ = self.pages.remove(&page_no);
        }
    }
    /// try to get result from the Load operation
    pub fn load_result(&mut self, page_no: usize) -> Option<Result<Vec<u8>, errors::Error>> {
        let Some(info) = self.pages.get(&page_no) else {
            panic!("page state must be set before load_result");
        };
        info.load_result.clone()
    }
    /// unsubscribe from the result of the Load operation
    /// caller must use this method only if [Self::load_start] returned Ok(PageLoadAction::Wait)
    pub fn wait_end(&mut self, page_no: usize) {
        let Some(info) = self.pages.get_mut(&page_no) else {
            panic!("page state must be set before load_result");
        };
        info.load_waits -= 1;
        if info.load_waits == 0 && info.load_result.is_some() {
            let _ = self.pages.remove(&page_no);
        }
    }
}

/// Guard which tracks states of the pages and properly deinit them on Drop
struct PageStatesGuard {
    page_states: Arc<Mutex<PageStates>>,
    pages_to_load: Vec<u32>,
    pages_to_wait: Vec<u32>,
    pages_to_write: Vec<u32>,
}

impl PageStatesGuard {
    pub fn new(states: &Arc<Mutex<PageStates>>) -> Self {
        Self {
            page_states: states.clone(),
            pages_to_load: vec![],
            pages_to_wait: vec![],
            pages_to_write: vec![],
        }
    }
    pub fn write_start(&mut self, page_no: usize) -> Result<(), errors::Error> {
        let mut page_states = self.page_states.lock().unwrap();
        page_states.write_start(page_no)?;
        self.pages_to_write.push(page_no as u32);
        Ok(())
    }
    pub fn write_end(&mut self, page_no: usize) {
        let mut page_states = self.page_states.lock().unwrap();
        page_states.write_end(page_no);
        self.pages_to_write.retain(|&p| p != page_no as u32);
    }
    pub fn load_start(&mut self, page_no: usize) -> Result<PageLoadAction, errors::Error> {
        let mut page_states = self.page_states.lock().unwrap();
        let action = page_states.load_start(page_no)?;
        match action {
            PageLoadAction::Load => self.pages_to_load.push(page_no as u32),
            PageLoadAction::Wait => self.pages_to_wait.push(page_no as u32),
        }
        Ok(action)
    }
    pub fn load_result(&mut self, page_no: usize) -> Option<Result<Vec<u8>, errors::Error>> {
        let mut page_states = self.page_states.lock().unwrap();
        page_states.load_result(page_no)
    }
    pub fn load_end(&mut self, page_no: usize, result: Result<Vec<u8>, errors::Error>) {
        let mut page_states = self.page_states.lock().unwrap();
        page_states.load_end(page_no, result);
        self.pages_to_load.retain(|&x| x != page_no as u32);
    }
    pub fn wait_end(&mut self, page_no: usize) {
        let mut page_states = self.page_states.lock().unwrap();
        page_states.wait_end(page_no);
        self.pages_to_wait.retain(|&x| x != page_no as u32);
    }
}

impl Drop for PageStatesGuard {
    #[allow(clippy::unnecessary_to_owned)]
    fn drop(&mut self) {
        for page_no in self.pages_to_write.to_vec() {
            self.write_end(page_no as usize);
        }
        for page_no in self.pages_to_wait.to_vec() {
            self.wait_end(page_no as usize);
        }
        for page_no in self.pages_to_load.to_vec() {
            self.load_end(
                page_no as usize,
                Err(errors::Error::DatabaseSyncEngineError(
                    "unable to properly load page".to_string(),
                )),
            );
        }
    }
}

pub struct LazyDatabaseStorage<IO: SyncEngineIo> {
    clean_file: Arc<dyn File>,
    dirty_file: Option<Arc<dyn File>>,
    sync_engine_io: SyncEngineIoStats<IO>,
    server_revision: String,
    page_states: Arc<Mutex<PageStates>>,
    opts: PartialSyncOpts,
}

impl<IO: SyncEngineIo> LazyDatabaseStorage<IO> {
    pub fn new(
        clean_file: Arc<dyn File>,
        dirty_file: Option<Arc<dyn File>>,
        sync_engine_io: SyncEngineIoStats<IO>,
        server_revision: String,
        opts: PartialSyncOpts,
    ) -> Self {
        Self {
            clean_file,
            dirty_file,
            sync_engine_io,
            server_revision,
            opts,
            page_states: Arc::new(Mutex::new(PageStates::new())),
        }
    }
}

/// load pages from the list [PageStatesGuard::pages_to_load] from the remote at given revision
/// returns page data for the completion_page if it is set - otherwise returns None
async fn lazy_load_pages<IO: SyncEngineIo, Ctx>(
    coro: &Coro<Ctx>,
    sync_engine_io: &SyncEngineIoStats<IO>,
    clean_file: Arc<dyn File>,
    dirty_file: Option<Arc<dyn File>>,
    page_states_guard: &mut PageStatesGuard,
    server_revision: &str,
    completion_page: Option<u32>,
) -> Result<Option<Vec<u8>>, errors::Error> {
    tracing::info!(
        "lazy_load_pages(pages={:?}, revision={})",
        &page_states_guard.pages_to_load,
        server_revision
    );
    let loaded = pull_pages_v1(
        coro,
        sync_engine_io,
        server_revision,
        &page_states_guard.pages_to_load,
    )
    .await?;

    let mut completion_data = None;

    let page_buffer = Arc::new(Buffer::new_temporary(PAGE_SIZE));
    for (page_id, page) in loaded {
        page_buffer.as_mut_slice().copy_from_slice(&page);

        if Some(page_id as u32) == completion_page {
            assert!(
                completion_data.is_none(),
                "completion_data must be set only once"
            );
            completion_data = Some(page.clone());
        }

        let page_offset = page_id * PAGE_SIZE as u64;
        if let Some(dirty_file) = &dirty_file {
            let dirty_c = dirty_file.pwrite(
                page_offset,
                page_buffer.clone(),
                Completion::new_write(|_| {}),
            )?;
            assert!(
                dirty_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );
        }

        let clean_c = clean_file.pwrite(
            page_offset,
            page_buffer.clone(),
            Completion::new_write(|_| {}),
        )?;
        assert!(
            clean_c.finished(),
            "LazyDatabaseStorage works only with sync IO"
        );

        if let Some(dirty_file) = &dirty_file {
            dirty_file.punch_hole(page_offset as usize, page.len())?;
        }
        page_states_guard.load_end(page_id as usize, Ok(page));
    }

    Ok(completion_data)
}

#[allow(clippy::too_many_arguments)]
async fn read_page<Ctx, IO: SyncEngineIo>(
    coro: &Coro<Ctx>,
    sync_engine_io: &SyncEngineIoStats<IO>,
    clean_file: Arc<dyn File>,
    dirty_file: Option<Arc<dyn File>>,
    guard: &mut PageStatesGuard,
    server_revision: &str,
    page: usize,
    segment_size: usize,
    prefetch: bool,
    c: Completion,
) -> Result<(), errors::Error> {
    let read_buf = c.as_read().buf().as_mut_slice();
    let read_buf_len = read_buf.len();
    assert!(read_buf_len <= PAGE_SIZE);

    // first, try to mark page as loading
    let page_action = guard.load_start(page)?;

    let data = if matches!(page_action, PageLoadAction::Wait) {
        tracing::info!("read_page(page={page}): wait for the page to load");
        // another connection already loading this page - so we need to wait
        loop {
            let _ = coro.yield_(crate::types::SyncEngineIoResult::IO).await;
            let Some(result) = guard.load_result(page) else {
                continue;
            };
            tracing::info!("read_page(page={page}): err={:?}", result.as_ref().err());
            let data = result?;
            assert!(data.len() == PAGE_SIZE);
            break data;
        }
    } else {
        tracing::info!(
            "read_page(page={page}, segment_size={segment_size}): read page from the remote server"
        );
        let segment_start = page * PAGE_SIZE / segment_size * segment_size;
        let segment_end = segment_start + segment_size;

        for segment_page in segment_start / PAGE_SIZE..segment_end / PAGE_SIZE {
            if page != segment_page {
                match guard.load_start(segment_page) {
                    Ok(PageLoadAction::Wait) => guard.wait_end(segment_page),
                    Ok(PageLoadAction::Load) => continue,
                    Err(_) => continue,
                }
            }
        }

        let Some(page_data) = lazy_load_pages(
            coro,
            sync_engine_io,
            clean_file.clone(),
            dirty_file.clone(),
            guard,
            server_revision,
            Some(page as u32),
        )
        .await?
        else {
            panic!("page_data must be set for completion");
        };
        page_data
    };

    let buffer = Arc::new(Buffer::new(data));
    if prefetch {
        tracing::info!("read_page(page={page}): trying to prefetch more pages");
        let content = PageContent::new(0, buffer.clone());
        if content.maybe_page_type().is_some() {
            tracing::info!(
                "read_page(page={page}): detected valid page for prefetch load: {:?}",
                content.maybe_page_type()
            );
            let mut page_refs = Vec::with_capacity(content.cell_count() + 1);
            for cell_id in 0..content.cell_count() {
                let Ok(cell) = content.cell_get(cell_id, PAGE_SIZE) else {
                    tracing::info!(
                        "read_page(page={page}): unable to parse cell at position {cell_id}"
                    );
                    break;
                };
                if let Some(pointer) = content.rightmost_pointer() {
                    page_refs.push(pointer);
                }
                match cell {
                    sqlite3_ondisk::BTreeCell::TableInteriorCell(cell) => {
                        page_refs.push(cell.left_child_page);
                    }
                    sqlite3_ondisk::BTreeCell::IndexInteriorCell(cell) => {
                        page_refs.push(cell.left_child_page);
                    }
                    sqlite3_ondisk::BTreeCell::TableLeafCell(..) => {}
                    sqlite3_ondisk::BTreeCell::IndexLeafCell(..) => {}
                };
            }
            let mut prefetch_pages = Vec::with_capacity(page_refs.len());
            for page_ref in page_refs {
                match guard.load_start(page_ref as usize) {
                    Ok(PageLoadAction::Load) => prefetch_pages.push(page_ref),
                    Ok(PageLoadAction::Wait) => guard.wait_end(page_ref as usize),
                    Err(err) => {
                        // the prefetch is an optimization; if we can't load the page this is fine
                        tracing::info!("read_page(page={page}): unable to lock page {page_ref} for prefetch load: {err}");
                    }
                }
            }
            lazy_load_pages(
                coro,
                sync_engine_io,
                clean_file,
                dirty_file,
                guard,
                server_revision,
                None,
            )
            .await?;
        }
    }

    tracing::info!("read_page(page={page}): page loaded");
    read_buf.copy_from_slice(&buffer.as_slice()[0..read_buf_len]);
    c.complete(read_buf_len as i32);
    Ok(())
}

impl<IO: SyncEngineIo> DatabaseStorage for LazyDatabaseStorage<IO> {
    fn read_header(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        assert!(
            !self.clean_file.has_hole(0, PAGE_SIZE)?,
            "first page must be filled"
        );
        self.clean_file.pread(0, c)
    }

    fn read_page(
        &self,
        page_idx: usize,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            io_ctx.encryption_context().is_none(),
            "encryption or checksum are not supported with partial sync"
        );
        assert!(page_idx > 0, "page should be positive");
        let r = c.as_read();
        let size = r.buf().len();
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(LimboError::NotADB);
        }

        let page = page_idx - 1;
        let read_buf = c.as_read().buf().as_mut_slice();
        let read_buf_len = read_buf.len();
        assert!(read_buf_len <= PAGE_SIZE);
        let Some(page_offset) = (page as u64).checked_mul(read_buf_len as u64) else {
            return Err(LimboError::IntegerOverflow);
        };

        // we can't put this logic in the generator below for now, because otherwise initialization of database will stuck
        // (the problem is that connection creation use blocking IO in some code pathes, and in this case we will be unable to spin sync engine specific callbacks)
        let is_hole = self
            .clean_file
            .has_hole(page_offset as usize, read_buf_len)?;

        tracing::info!("read_page(page={}): is_hole={}", page, is_hole);
        if !is_hole {
            let Some(dirty_file) = &self.dirty_file else {
                // no dirty file was set - this means that FS is atomic (e.g. MemoryIO)
                return self.clean_file.pread(page_offset, c);
            };
            if dirty_file.has_hole(page_offset as usize, size)? {
                // dirty file has no hole - this means that we cleanly removed the hole when we wrote to the clean file
                return self.clean_file.pread(page_offset, c);
            }
            let check_buffer = Arc::new(Buffer::new_temporary(size));
            let check_c = dirty_file.pread(
                page_offset,
                Completion::new_read(check_buffer.clone(), |_| {}),
            )?;
            assert!(
                check_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );

            let clean_buffer = r.buf_arc();
            let clean_c = self.clean_file.pread(
                page_offset,
                Completion::new_read(clean_buffer.clone(), |_| {}),
            )?;
            assert!(
                clean_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );

            if check_buffer.as_slice().eq(clean_buffer.as_slice()) {
                // dirty buffer matches clean buffer - this means that clean data is valid
                return self.clean_file.pread(page_offset, c);
            }
        }

        let mut generator = genawaiter::sync::Gen::new({
            let sync_engine_io = self.sync_engine_io.clone();
            let server_revision = self.server_revision.clone();
            let clean_file = self.clean_file.clone();
            let dirty_file = self.dirty_file.clone();
            let page_states = self.page_states.clone();
            let segment_size = self.opts.segment_size();
            let prefetch = self.opts.prefetch;
            let c = c.clone();
            move |coro| async move {
                let coro = Coro::new((), coro);
                let mut guard = PageStatesGuard::new(&page_states);
                read_page(
                    &coro,
                    &sync_engine_io,
                    clean_file,
                    dirty_file,
                    &mut guard,
                    &server_revision,
                    page_idx - 1,
                    segment_size,
                    prefetch,
                    c,
                )
                .await?;
                tracing::info!(
                    "PartialDatabaseStorage::read_page(page={}): page read succeeded",
                    page
                );
                Ok::<(), errors::Error>(())
            }
        });
        self.sync_engine_io.add_io_callback(Box::new(move || {
            match generator.resume_with(Ok(())) {
                genawaiter::GeneratorState::Yielded(_) => false,
                genawaiter::GeneratorState::Complete(_) => true,
            }
        }));
        Ok(c)
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: std::sync::Arc<turso_core::Buffer>,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            io_ctx.encryption_context().is_none(),
            "encryption or checksum are not supported with partial sync"
        );

        let buffer_size = buffer.len();
        assert!(page_idx > 0);
        assert!(buffer_size >= 512);
        assert!(buffer_size <= 65536);
        assert_eq!(buffer_size & (buffer_size - 1), 0);
        let Some(pos) = (page_idx as u64 - 1).checked_mul(buffer_size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };

        let mut guard = PageStatesGuard::new(&self.page_states);
        guard.write_start(page_idx - 1).map_err(|e| {
            LimboError::InternalError(format!("failed to get write lock for a page: {e}"))
        })?;

        // we write to the database only during checkpoint - so we need to punch hole in the dirty file in order to mark this region as valid
        if let Some(dirty_file) = &self.dirty_file {
            dirty_file.punch_hole(pos as usize, buffer_size)?;
        }
        self.clean_file.pwrite(pos, buffer, c)
    }

    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<std::sync::Arc<turso_core::Buffer>>,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        assert!(
            io_ctx.encryption_context().is_none(),
            "encryption or checksum are not supported with partial sync"
        );

        assert!(first_page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert_eq!(page_size & (page_size - 1), 0);

        let Some(pos) = (first_page_idx as u64 - 1).checked_mul(page_size as u64) else {
            return Err(LimboError::IntegerOverflow);
        };
        // we write to the database only during checkpoint - so we need to punch hole in the dirty file in order to mark this region as valid
        if let Some(dirty_file) = &self.dirty_file {
            let buffers_size = buffers.iter().map(|b| b.len()).sum();
            dirty_file.punch_hole(pos as usize, buffers_size)?;
        }
        let c = self.clean_file.pwritev(pos, buffers, c)?;
        Ok(c)
    }

    fn sync(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        if let Some(dirty_file) = &self.dirty_file {
            let dirty_c = dirty_file.sync(Completion::new_sync(|_| {}))?;
            assert!(
                dirty_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );
        }

        self.clean_file.sync(c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.clean_file.size()
    }

    fn truncate(
        &self,
        len: usize,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        if let Some(dirty_file) = &self.dirty_file {
            let dirty_c = dirty_file.truncate(len as u64, Completion::new_trunc(|_| {}))?;
            assert!(
                dirty_c.finished(),
                "LazyDatabaseStorage works only with sync IO"
            );
        }

        self.clean_file.truncate(len as u64, c)
    }
}
