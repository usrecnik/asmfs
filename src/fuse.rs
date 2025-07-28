use fuser::{FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request};
use libc::{ENOENT}; /* ENOSYS */
use std::ffi::OsStr;
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};
use log::{debug, info, error}; // debug
use crate::oracle::OracleConnection;
use oracle::{Error};


const TTL: Duration = Duration::from_secs(60); // 1 minute


struct OpenFileHandle {
    conn: OracleConnection,
    block_size: u32,
    file_size: u64, // in number of blocks
    file_type: u32
}

impl OpenFileHandle {
    pub fn bytes_size(&self) -> u64 {
        self.block_size as u64 * self.file_size
    }
}

pub struct AsmFS {
    ora: OracleConnection,
    mount_point: String,
    handles: HashMap<u64, OpenFileHandle> // see open() and close()
}

impl AsmFS {
    pub fn new(mut mount_point: String) -> Self {
        if !mount_point.ends_with("/") {
            mount_point.push('/');
        }

        info!("Connecting to oracle...");
        let ora = match OracleConnection::connect() {
            Ok(ora) => ora,
            Err(e) => {
                error!("Unable to connect to oracle: {}", e);
                std::process::exit(1);
            }
        };

        AsmFS { ora, mount_point, handles: HashMap::new() }
    }
}

impl Filesystem for AsmFS {

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {

        info!("readdir(ino={}, offset={}, _fh={})", ino, offset, _fh);

        let contents: Result<Vec<(u64, FileType, String)>, Error>;
        if ino == 1 {
            debug!(".. readdir() ok");
            contents = self.ora.query_asm_diskgroup_vec();
        } else {
            debug!(".. readdir() failed: {}", ino);
            contents = self.ora.query_asm_alias_vec(ino);
        }

        match contents {
            Ok(dg_vec) => {
                for (i, entry) in dg_vec.into_iter().enumerate().skip(offset as usize) {
                    if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                        break;
                    }
                }
            }
            Err(e) => {
                error!("readdir() failed: {}", e);
                reply.error(ENOENT);
                return;
            }
        }

        reply.ok();
    }

    /*
     Look up a directory entry by name and get its attributes.
     _req: Metadata about the FUSE request, including user ID, group ID, process ID, etc.
     parent: The inode number (or file handle) of the parent directory in which to look for name.
     name: The name of the directory entry being looked up (e.g., "foo.txt")
     */
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().expect("unable to convert OsStr to str");

        info!("lookup(parent={}, name={})", parent, name_str);

        let contents: Result<FileAttr, Error>;

        if parent == 1 {
            contents = self.ora.query_asm_diskgroup_ent_name(&name_str);
        } else {
            contents = self.ora.query_asm_alias_ent(parent, &name_str);
        }

        match contents {
            Ok(attr) => {
                debug!(".. lookup() ok");
                reply.entry(&TTL, &attr, 0);
                return;
            },
            Err(e) => {
                error!(".. lookup() failed: {}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {

        info!("getattr(ino={})", ino);

        if ino == 1 {
            let root = FileAttr {
                ino: 1,
                size: 0,
                blocks: 0,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            return reply.attr(&TTL, &root);

        } else if ino < 2000 {
            return reply.attr(&TTL, &self.ora.query_asm_diskgroup_ent_ino(ino));
        } else {
            let tmp = match self.ora.query_asm_alias_ent_ino(ino) {
                Ok(entry) => entry,
                Err(e) => {
                    error!("query asm$alias failed: {}", e);
                    return reply.error(ENOENT);
                }
            };

            return reply.attr(&TTL, &tmp);
        }
        //return reply.error(ENOENT)
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        info!("readlink(ino={})", ino);
        match self.ora.query_asm_alias_link(ino) {
            Ok(target) => {
                let abs_target: String = format!("{}{}", self.mount_point, target);
                debug!(".. readlink() ok, target={}", abs_target);
                reply.data(abs_target.as_bytes());
            },
            Err(e) => {
                error!(".. readlink() failed: {}", e);
                reply.error(ENOENT);
                return;
            }
        };
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        info!("open(ino={})", ino);

        // each call to open() establishes new connection
        let conn = match OracleConnection::connect() {
            Ok(ora) => ora,
            Err(e) => {
                error!("open() failed establishing new connection: {}", e);
                reply.error(ENOENT);
                return;
            }
        };

        match conn.proc_open(ino) {
            Ok(data) => {
                let handle = OpenFileHandle {
                    conn,
                    block_size: data.1,
                    file_size: data.2,
                    file_type: data.3
                };

                self.handles.insert(data.0, handle);

                reply.opened(data.0, 0);
                debug!(".. open() ok, fh={}", data.0);
            },
            Err(e) => {
                error!(".. open() failed: {}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn release(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        info!("release(fh={})", fh);

        let handle = self.handles.get(&fh).unwrap();
        match handle.conn.proc_close(fh) {
            Ok(()) => {
                reply.ok();
                debug!(".. release() ok");
            },
            Err(e) => {
                error!(".. release() failed: {}", e);
            }
        }
        self.handles.remove(&fh);
    }

    fn read(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, size: u32, _flags: i32, _lock: Option<u64>, reply: ReplyData) {
        info!("read(ino={}, _fh={}, offset={}, _size={}, flags={})", ino, fh, offset, size, _flags);
        let handle = self.handles.get(&fh).unwrap();
        
        match handle.conn.proc_read(fh, offset, size, handle.block_size, handle.bytes_size(), handle.file_type) {
            Ok(buffer) => {
                reply.data(buffer.as_slice());
                debug!(".. read() ok, offset={}, size={}", offset, size);
            },
            Err(e) => {
                error!("read() failed: {}", e);
                reply.error(ENOENT);
            }
        }
    }
}