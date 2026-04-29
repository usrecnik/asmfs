use fuser::{Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, LockOwner, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request};
use std::ffi::OsStr;
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex, RwLock};
use log::{debug, info, error}; // debug
use crate::oracle::{OracleConnection, RawOpenFileHandle, ASM_STRIPED_COARSE, ASM_STRIPED_FINE};
use oracle::{Error, ErrorKind};


const TTL: Duration = Duration::from_secs(60); // 1 minute

const MAGIC_FILE_TYPES: &[(&str, u32, u32, u32)] = &[  // file_type, magic_constant, version min, version max
        ("ARCHIVELOG",  0x0000_81A0,     0,  19999), // not needed since, at last 23.26.1 onward (19c only)
        ("DATAFILE",    0x0000_81A0,     0,  19999), // <= 19c
        ("DATAFILE",    0x0000_0002, 20000, 999999), // >= 26ai
        ("TEMPFILE",    0x0000_81A0,     0,  19999), // not needed since, at last 23.26.1 onward (19c only)
        ("CONTROLFILE", 0x0000_0002,     0, 999999)
];
// TEMPFILEs needs no fix.
// ARCHIVELOG in 26ai needs no fix.

struct OpenFileHandle {
    conn: OracleConnection,
    block_size: u32,
    blocks_asm: u64,
    blocks_fs: u64,
    file_type: u32
}

impl OpenFileHandle {
    pub fn bytes_size_fs(&self) -> u64 {
        self.block_size as u64 * self.blocks_fs
    }
    pub fn bytes_size_asm(&self) -> u64 {
        self.block_size as u64 * self.blocks_asm
    }
}

pub struct AsmFS {
    ora: Mutex<OracleConnection>,
    connection_string: Option<String>,      // read-only after init
    mount_point: String,                    // read-only after init
    handles_dbms: Mutex<HashMap<u64, OpenFileHandle>>,
    handles_raw: RwLock<HashMap<u64, Arc<RawOpenFileHandle>>>,
    use_raw: bool,  // read only after init
    mirror: u8,     // read only after init
    magic: bool,    // read only after init
    oracle_version: u32 // only written in constructor
}

impl AsmFS {
    pub fn new(mut mount_point: String, connection_string: Option<String>, use_raw: bool, magic: bool, mirror: u8) -> Self {
        if !mount_point.ends_with("/") {
            mount_point.push('/');
        }

        info!("Connecting to oracle...");
        let ora = match OracleConnection::connect(connection_string.clone()) {
            Ok(ora) => ora,
            Err(e) => {
                error!("Unable to connect to oracle: {}", e);
                std::process::exit(1);
            }
        };

        let oracle_version: u32 = match ora.query_oracle_version() {
            Ok(version) => version,
            Err(e) => {
                error!("Unable to query oracle major version: {}", e);
                std::process::exit(1);
            }
        };

        AsmFS {
            ora: Mutex::new(ora),
            connection_string,
            mount_point,
            handles_dbms: Mutex::new(HashMap::new()),
            handles_raw: RwLock::new(HashMap::new()),
            use_raw,
            mirror,
            magic,
            oracle_version}
    }
}

impl Filesystem for AsmFS {

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_str().expect("unable to convert OsStr to str");

        info!("lookup(parent={}, name={})", parent, name_str);

        let contents: Result<FileAttr, Error>;

        if parent.0 == 1 {
            contents = self.ora.lock().unwrap().query_asm_diskgroup_ent_name(&name_str);
        } else {
            contents = self.ora.lock().unwrap().query_asm_alias_ent(parent.0, &name_str);
        }

        match contents {
            Ok(attr) => {
                debug!(".. lookup() ok");
                reply.entry(&TTL, &attr, Generation(0));
                return;
            },
            Err(e) => {
                error!(".. lookup() failed: {}", e);
                reply.error(Errno::ENOENT);
            }
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {

        info!("getattr(ino={})", ino);

        if ino.0 == 1 {
            let root = FileAttr {
                ino: INodeNo(1),
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

        } else if ino.0 < 2000 {
            return reply.attr(&TTL, &self.ora.lock().unwrap().query_asm_diskgroup_ent_ino(ino.0));
        } else {
            let tmp = match self.ora.lock().unwrap().query_asm_alias_ent_ino(ino.0) {
                Ok(entry) => entry,
                Err(e) => {
                    error!("query asm$alias failed: {}", e);
                    return reply.error(Errno::ENOENT);
                }
            };

            return reply.attr(&TTL, &tmp);
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        info!("readlink(ino={})", ino);
        match self.ora.lock().unwrap().query_asm_alias_link(ino.0) {
            Ok(target) => {
                let abs_target: String = format!("{}{}", self.mount_point, target);
                debug!(".. readlink() ok, target={}", abs_target);
                reply.data(abs_target.as_bytes());
            },
            Err(e) => {
                error!(".. readlink() failed: {}", e);
                reply.error(Errno::ENOENT);
                return;
            }
        };
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        info!("open(ino={})", ino);

        if self.use_raw {
            self.open_raw(_req, ino.0, _flags, reply);
        } else {
            self.open_dbms(_req, ino.0, _flags, reply);
        }
    }

    fn read(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock: Option<LockOwner>, reply: ReplyData) {
        // info!("read(ino={}, _fh={}, offset={}, _size={}, flags={})", ino, fh, offset, size, _flags);
        if self.use_raw {
            let handle = {
                let guard = self.handles_raw.read().unwrap();
                match guard.get(&fh.0) {
                    Some(h) => Arc::clone(h),
                    None => {reply.error(Errno::EBADF); return;}
                }
            };

            if handle.striped == ASM_STRIPED_COARSE {
                self.read_raw_coarse(handle, offset, size, reply);
            } else if handle.striped == ASM_STRIPED_FINE {
                self.read_raw_fine(handle, offset, size, reply);
            } else {
                error!("Unsupported stripped mode: {}", handle.striped);
                reply.error(Errno::EINVAL);
            }
        } else {
            self.read_dbms(_req, ino.0, fh.0, offset, size, _flags, _lock, reply);
        }
    }

    fn release(&self, _req: &Request, ino: INodeNo, fh: FileHandle, _flags: OpenFlags, _lock_owner: Option<LockOwner>, _flush: bool, reply: ReplyEmpty) {
        info!("release(fh={})", fh);

        if self.use_raw {
            self.release_raw(_req, ino.0, reply);
        } else {
            self.release_dbms(_req, fh.0, reply);
        }
    }

    fn readdir(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {

        info!("readdir(ino={}, offset={}, _fh={})", ino, offset, _fh);
        let contents: Result<Vec<(u64, FileType, String)>, Error>;

        if ino.0 == 1 {
            debug!(".. readdir() ok");
            contents = self.ora.lock().unwrap().query_asm_diskgroup_vec();
        } else {
            debug!(".. readdir() failed: {}", ino);
            contents = self.ora.lock().unwrap().query_asm_alias_vec(ino.0);
        }

        match contents {
            Ok(dg_vec) => {
                for (i, entry) in dg_vec.into_iter().enumerate().skip(offset as usize) {

                    if reply.add(INodeNo(entry.0), (i + 1) as u64, entry.1, entry.2) {
                        break;
                    }
                }
            }
            Err(e) => {
                error!("readdir() failed: {}", e);
                //reply.error(ENOENT);
                return;
            }
        }

        reply.ok();
    }
}

impl AsmFS {
    fn open_dbms(&self, _req: &Request, ino: u64, _flags: OpenFlags, reply: ReplyOpen) {
        // each call to open() establishes new connection
        let conn = match OracleConnection::connect(self.connection_string.clone()) {
            Ok(ora) => ora,
            Err(e) => {
                error!("open() failed establishing new connection: {}", e);
                reply.error(Errno::ENOENT);
                return;
            }
        };

        match conn.proc_open(ino) {
            Ok(data) => {
                let handle = OpenFileHandle {
                    conn,
                    block_size: data.1,
                    blocks_asm: data.2,
                    blocks_fs: data.3,
                    file_type: data.4
                };

                self.handles_dbms.lock().unwrap().insert(data.0, handle);

                reply.opened(FileHandle(data.0), FopenFlags::empty());
                debug!(".. open() ok, fh={}", data.0);
            },
            Err(e) => {
                error!(".. open() failed: {}", e);
                reply.error(Errno::ENOENT);
            }
        }
    }

    fn open_raw(&self, _req: &Request, ino: u64, _flags: OpenFlags, reply: ReplyOpen) {
        let h = self.ora.lock().unwrap().proc_open_raw(ino, self.mirror);
        match h {
            Ok(handle) => {
                let file_number :u32 = handle.file_number;

                self.handles_raw
                    .write()
                    .unwrap()
                    .insert(ino, Arc::new(handle));

                debug!(".. open() ok, fh={}, file_number={}", ino, file_number);
                reply.opened(FileHandle(ino), FopenFlags::empty());
            },
            Err(e) => {
                error!(".. open() failed: {}", e);
                reply.error(Errno::ENOENT)
            }
        }
    }

    fn release_dbms(&self, _req: &Request, fh: u64, reply: ReplyEmpty) {
        let mut guard = self.handles_dbms.lock().unwrap();
        let handle = guard.get(&fh).unwrap();
        match handle.conn.proc_close(fh) {
            Ok(()) => {
                reply.ok();
                debug!(".. release() ok");
            },
            Err(e) => {
                error!(".. release() failed: {}", e);
            }
        }
        guard.remove(&fh);
    }

    fn release_raw(&self, _req: &Request<>, ino: u64, reply: ReplyEmpty) {
        self.handles_raw.write().unwrap().remove(&ino);
        reply.ok();
        debug!(".. release() ok");
    }

    fn read_dbms(&self, _req: &Request, _ino: u64, fh: u64, offset: u64, size: u32, _flags: OpenFlags, _lock: Option<LockOwner>, reply: ReplyData) {
        let guard = self.handles_dbms.lock().unwrap();
        let handle = guard.get(&fh).unwrap();

        match handle.conn.proc_read(fh, offset, size, handle.block_size, handle.bytes_size_fs(), handle.bytes_size_asm(), handle.file_type) {
            Ok(buffer) => {
                reply.data(buffer.as_slice());
                debug!(".. read() ok, offset={}, size={}", offset, size);
            },
            Err(e) => {
                error!("read() failed: {}", e);
                reply.error(Errno::ENOENT);
            }
        }
    }

    fn read_raw_fine(&self, _handle: Arc<RawOpenFileHandle>, _offset: u64, _bytes_requested: u32, reply: ReplyData) {
        error!("read_raw_fine() not implemented");
        reply.error(Errno::ENOSYS);
    }

    fn read_raw_coarse(&self, handle: Arc<RawOpenFileHandle>, offset: u64, bytes_requested: u32, reply: ReplyData) {

        // clamp requested size to file size
        let size: usize = {
            let s = bytes_requested as u64;
            let clamped = if offset + s > handle.file_size_bytes {
                handle.file_size_bytes.saturating_sub(offset)
            } else {
                s
            };
            clamped as usize
        };

        if size == 0 {
            reply.data(&[]);
            return;
        }

        let au_size = handle.au_size as u64;
        let au_first = offset / au_size;
        let au_last  = (offset + size as u64 - 1) / au_size;

        // single allocation for the whole reply
        let mut buffer = vec![0u8; size];
        let mut bytes_read: usize = 0;

        if au_last as usize >= handle.au_list.len() {
            error!("AU {} not found in extent map (map len={}, file_number={})", au_last, handle.au_list.len(), handle.file_number);
            reply.error(Errno::EIO);
            return;
        }

        for au_index in au_first..=au_last {
            let first_byte: u32 = if au_index == au_first {
                (offset % au_size) as u32
            } else {
                0
            };

            // we can read at most (au_size - first_byte) bytes from this AU,
            // and we need at most (size - bytes_read) total
            let au_remaining = handle.au_size as usize - first_byte as usize;
            let still_needed = size - bytes_read;
            let chunk_len = std::cmp::min(au_remaining, still_needed);

            let au_entry = handle.au_list[au_index as usize];
            let file_handle = handle.disk_list.get(&au_entry.0).unwrap();
            let disk_offset = au_entry.1 as u64 * au_size + first_byte as u64;

            file_handle
                .read_exact_at(&mut buffer[bytes_read..bytes_read + chunk_len], disk_offset)
                .expect("read_exact_at() failed");

            bytes_read += chunk_len;
        }

        if self.magic && offset == 0 && au_first == 0 {
            if let Some((_, magic_constant, _, _)) = MAGIC_FILE_TYPES.iter().find(|(file_type, _, ver_min, ver_max)|
                *file_type == handle.file_type.as_str() && (&self.oracle_version >= ver_min && &self.oracle_version <= ver_max)
            ) {
                if let Err(e) = fix_header_block(&mut buffer, *magic_constant) {
                    error!(".. read_raw() failed to fix header block: {}", e);
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        }

        reply.data(&buffer);
    }
}

// this works on datafiles:
fn fix_header_block(buffer: &mut Vec<u8>, target_metadata: u32) -> Result<(), Error> {

    info!("Fixing header block with target_metadata: 0x{:08X}", target_metadata);

    if buffer.len() < 512 {
        return Err(Error::new(ErrorKind::Other, "asmfs; archivelog header buffer is less than 512 bytes"));
    }

    let metadata_bytes: [u8; 4] = buffer[0x20..0x24]
        .try_into()
        .map_err(|_| Error::new(ErrorKind::Other, "Failed to read metadata 0x20 -> 0x24"))?;
    let metadata = u32::from_le_bytes(metadata_bytes);
    let delta = metadata ^ target_metadata;

    let checksum_bytes: [u8; 4] = buffer[0x10..0x14]
        .try_into()
        .map_err(|_| Error::new(ErrorKind::Other, "Failed to read checksum 0x10 -> 0x14"))?;
    let checksum = u32::from_le_bytes(checksum_bytes) ^ delta;
    buffer[0x10..0x14].copy_from_slice(&checksum.to_le_bytes());

    buffer[0x20..0x24].copy_from_slice(&target_metadata.to_le_bytes());
    Ok(())
}