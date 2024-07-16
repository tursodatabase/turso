use super::{Completion, File, WriteCompletion, IO};
use anyhow::{ensure, Result};
use log::trace;
use std::cell::RefCell;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;
use std::{fmt, fs};
use thiserror::Error;
use libc::{c_void, iovec};
use libc;
use nix::sys::statfs::{statfs, Statfs};
use nix::sys::statvfs::{statvfs, FsFlags};

#[derive(Debug, Error)]
enum LinuxIOError {
    IOUringCQError(i32),
}

// Implement the Display trait to customize error messages
impl fmt::Display for LinuxIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinuxIOError::IOUringCQError(code) => write!(f, "IOUring completion queue error occurred with code {}", code),
        }
    }
}

pub struct LinuxIO {
    ring: Rc<RefCell<io_uring::IoUring>>,
}

impl LinuxIO {
    pub fn new() -> Result<Self> {
        let ring = io_uring::IoUring::new(128)?;
        Ok(Self {
            ring: Rc::new(RefCell::new(ring)),
        })
    }
}

impl IO for LinuxIO {
    fn open_file(&self, path: &str) -> Result<Rc<dyn File>> {
        let stat = statvfs(path).unwrap();
        trace!("open_file(path = {}, flags = {:#?})", path,  stat.flags());
        /*if stat.flags() & vfs_flags::ST_RDONLY != 0 {
            println!("Filesystem at {} is read-only.", path);
        } else {
            // Check for IO_DIRECT flag
            if stat.f_flags & nix::sys::statvfs::vfs_flags::ST_NOATIME != 0 {
                println!("Filesystem at {} supports IO_DIRECT flag.", path);
            } else {
                println!("Filesystem at {} does not support IO_DIRECT flag.", path);
            }
        }*/

        //trace!("open_file(path = {}, fstype = {:#?})", path, fstype);
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            //.custom_flags(libc::O_DIRECT)
            .open(path)?;
        Ok(Rc::new(LinuxFile {
            ring: self.ring.clone(),
            file,
        }))
    }

    fn run_once(&self) -> Result<()> {
        trace!("run_once()");
        let mut ring = self.ring.borrow_mut();
        ring.submit_and_wait(1)?;
        while let Some(cqe) = ring.completion().next() {
            let result = cqe.result();
            ensure!(
                result >= 0,
                LinuxIOError::IOUringCQError(result)
            );
            //trace!("Read {} bytes", result);
            let c = unsafe { Rc::from_raw(cqe.user_data() as *const Completion) };
            c.complete();
        }
        Ok(())
    }
}

pub struct LinuxFile {
    ring: Rc<RefCell<io_uring::IoUring>>,
    file: std::fs::File,
}

impl File for LinuxFile {
    fn pread(&self, pos: usize, c: Rc<Completion>) -> Result<()> {
        trace!("pread(pos = {}, length = {})", pos, c.buf().len());
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let read_e = {
            let mut buf = c.buf_mut();
            let len = buf.len();
            let buf = buf.as_mut_ptr();
            let ptr: *const Completion = Rc::into_raw(c.clone());
            //TODO: Check where to instantiate Probe
            //let probe = io_uring::Probe::new();
            //if probe.is_supported(io_uring::opcode::Read::CODE) {
            io_uring::opcode::Read::new(fd, buf, len as u32)
            .offset(pos as u64)
            .build()
            .user_data(ptr as u64)
            /*let iovec = [libc::iovec {
                iov_base: buf as *mut libc::c_void,
                iov_len: len,
            }];
            io_uring::opcode::Readv::new(fd, iovec.as_ptr(), iovec.len() as u32)
            //.offset(pos as u64)
            .build()
            .user_data(ptr as u64)*/
            /*} else {
                let iovec = [libc::iovec {
                    iov_base: buf as *mut libc::c_void,
                    iov_len: len,
                }];
                io_uring::opcode::Readv::new(fd, iovec.as_ptr(), iovec.len() as u32)
                .offset(pos as u64)
                .build()
                .user_data(ptr as u64)
            }*/
        };
        let mut ring = self.ring.borrow_mut();
        unsafe {
            ring.submission()
                .push(&read_e)
                .expect("submission queue is full");
        }
        Ok(())
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<RefCell<crate::Buffer>>,
        c: Rc<WriteCompletion>,
    ) -> Result<()> {
        let fd = io_uring::types::Fd(self.file.as_raw_fd());
        let write = {
            let buf = buffer.borrow();
            let ptr = Rc::into_raw(c.clone());
            io_uring::opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                .offset(pos as u64)
                .build()
                .user_data(ptr as u64)
        };
        let mut ring = self.ring.borrow_mut();
        unsafe {
            ring.submission()
                .push(&write)
                .expect("submission queue is full");
        }
        Ok(())
    }
}
