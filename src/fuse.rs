use fuser::{FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request};
use libc::{ENOENT}; /* ENOSYS, EINVAL */
use std::ffi::OsStr;
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use log::{debug, info, error}; // debug
use crate::oracle::{OracleConnection, RawOpenFileHandle};
use oracle::{Error, ErrorKind};


const TTL: Duration = Duration::from_secs(60); // 1 minute

const MAGIC_FILE_TYPES: &[&str] = &["ARCHIVELOG", "DATAFILE", "TEMPFILE", "CONTROLFILE"];

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
    ora: OracleConnection,
    connection_string: Option<String>,
    mount_point: String,
    handles: HashMap<u64, OpenFileHandle>, // see open() and close()
    handles_raw: HashMap<u64, RawOpenFileHandle>,
    use_raw: bool,
    mirror: u8
}

impl AsmFS {
    pub fn new(mut mount_point: String, connection_string: Option<String>, use_raw: bool, mirror: u8) -> Self {
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

        AsmFS {
            ora,
            connection_string,
            mount_point,
            handles: HashMap::new(),
            handles_raw: HashMap::new(),
            use_raw,
            mirror }
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

        if self.use_raw {
            self.open_raw(_req, ino, _flags, reply);
        } else {
            self.open_dbms(_req, ino, _flags, reply);
        }
    }

    fn release(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        info!("release(fh={})", fh);

        if self.use_raw {
            self.release_raw(_req, _ino, fh, _flags, _lock_owner, _flush, reply);
        } else {
            self.release_dbms(_req, _ino, fh, _flags, _lock_owner, _flush, reply);
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, size: u32, _flags: i32, _lock: Option<u64>, reply: ReplyData) {
        info!("read(ino={}, _fh={}, offset={}, _size={}, flags={})", ino, fh, offset, size, _flags);
        if self.use_raw {
            self.read_raw(_req, ino, fh, offset, size, _flags, _lock, reply);
        } else {
            self.read_dbms(_req, ino, fh, offset, size, _flags, _lock, reply);
        }
    }
}

impl AsmFS {
    fn open_dbms(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        // each call to open() establishes new connection
        let conn = match OracleConnection::connect(self.connection_string.clone()) {
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
                    blocks_asm: data.2,
                    blocks_fs: data.3,
                    file_type: data.4
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

    fn open_raw(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let h = self.ora.proc_open_raw(ino, self.mirror);
        match h {
            Ok(handle) => {
                self.handles_raw.insert(ino, handle);
                debug!(".. open() ok, fh={}", ino);
                reply.opened(ino, 0);
            },
            Err(e) => {
                error!(".. open() failed: {}", e);
                reply.error(ENOENT)
            }
        }
    }

    fn release_dbms(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
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

    fn release_raw(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, _lock_owner: Option<u64>, _flush: bool, reply: ReplyEmpty) {
        self.handles_raw.remove(&_ino);
        reply.ok();
        debug!(".. release() ok");
    }

    fn read_dbms(&mut self, _req: &Request, _ino: u64, fh: u64, offset: i64, size: u32, _flags: i32, _lock: Option<u64>, reply: ReplyData) {
        let handle = self.handles.get(&fh).unwrap();

        match handle.conn.proc_read(fh, offset, size, handle.block_size, handle.bytes_size_fs(), handle.bytes_size_asm(), handle.file_type) {
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

    fn read_raw(&mut self, _req: &Request, _ino: u64, fh: u64, offset: i64, bytes_requested: u32, _flags: i32, _lock: Option<u64>, reply: ReplyData) {
        let handle = self.handles_raw.get(&fh).unwrap();
        info!("read_raw() (offset={}, bytes_requested={}, file_size={}, au_size={})", offset, bytes_requested, handle.file_size_bytes, handle.au_size);

        let mut size :i64 = bytes_requested as i64;
        if offset as u64 + size as u64 > handle.file_size_bytes {
            info!(".. requested size in bytes is beyond file size (offset={}, size={}, file_size={})", offset, size, handle.file_size_bytes);
            size = (handle.file_size_bytes - offset as u64) as i64;
            if size <= 0 {
                size = 0;
            }
            info!(".. this changed size to: (offset={}, (!)size={}, file_size={})", offset, size, handle.file_size_bytes);
        }

        let au_first = offset / handle.au_size as i64;
        let au_last = au_first + (size / handle.au_size as i64);

        info!(".. read_raw() au_first={} (offset={} / au_size={})", au_first, offset, handle.au_size);
        info!(".. read_raw() au_last={} (au_first={} + (size={} / au_size={})", au_last, au_first, size, handle.au_size);

        let mut buffer :Vec<u8> = Vec::with_capacity(size as usize);

        info!(".. read_raw() begin loop through au_first={} to au_last={}", au_first, au_last);
        let mut bytes_read :usize = 0;
        for au_index in au_first .. au_last+1 { // `for x in from . to` (from is inclusive, to is exclusive), this is why +1 for au_last
            info!(".... read_raw() au_index={} (au_first={}, au_last={})", au_index, au_first, au_last);
            let first_byte :u32 = if au_index == au_first {
                offset as u32 % handle.au_size
            } else {
                0
            };
            info!("...... first_byte={} (offset={} % au_size={})", first_byte, offset, handle.au_size);

            let bytes_since_first = if size - (bytes_read as i64) < handle.au_size as i64 {
                size - bytes_read as i64
            } else {
                handle.au_size as i64
            };
            info!("...... bytes_since_first={}", bytes_since_first);

            let au_entry = handle.au_list[au_index as usize];
            info!("...... au={} (au offset in given block device)", au_entry.1);
            let block_device = handle.disk_list.get(&au_entry.0).unwrap();
            info!("...... block_device={}", block_device);

            let au_buf = read_raw_int(block_device, handle.au_size, au_entry.1, first_byte, bytes_since_first as u32).expect("read_raw_int() failed");
            info!("...... got buffer={}", au_buf.len());

            bytes_read = bytes_read + au_buf.len();
            buffer.extend(au_buf)
        }

        if offset == 0 && au_first == 0 && MAGIC_FILE_TYPES.contains(&handle.file_type.as_str()) {
            // this buffer contains the first block of the file
            match fix_header_block(&mut buffer) {
                Ok(()) => {},
                Err(e) => {
                    error!(".. read_raw() failed to fix header block: {}", e);
                    reply.error(ENOENT);
                    return;
                }
            }
        }

        info!(".. read_raw() sending reply");
        reply.data(buffer.as_slice());
        info!(".. read_raw()");
    }
}

fn read_raw_int(block_device: &str, au_size: u32, allocation_unit: u32, first_byte: u32, bytes_since_first: u32) -> io::Result<Vec<u8>>
{
    info!(".. read_raw_int() au_size={}, allocation_unit={}, first_byte={}, bytes_since_first={}", au_size, allocation_unit, first_byte, bytes_since_first);
    let offset = (allocation_unit as u64 * au_size as u64) + first_byte as u64;
    let length = bytes_since_first as usize;

    let mut file = File::open(block_device)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buffer = vec![0u8; length];
    file.read_exact(&mut buffer)?;

    Ok(buffer)
}

fn fix_header_block(buffer: &mut Vec<u8>) -> Result<(), Error> {
    println!(".. fix_header_block_archivelog begin");

    if buffer.len() < 512 {
        return Err(Error::new(ErrorKind::Other, "asmfs; archivelog header buffer is less than 512 bytes"));
    }

    // Fix checksum at 0x10 -> 0x14
    const MAGIC_XOR: u32 = 0x0000_81a0;

    let checksum_bytes: [u8; 4] = buffer[0x10..0x14]
        .try_into()
        .map_err(|_| Error::new(ErrorKind::Other, "Failed to read checksum 0x10 -> 0x14"))?;

    let checksum = u32::from_le_bytes(checksum_bytes) ^ MAGIC_XOR;
    buffer[0x10..0x14].copy_from_slice(&checksum.to_le_bytes());

    // Fix metadata at offset 0x20 -> 0x24
    buffer[0x20..0x24].copy_from_slice(&MAGIC_XOR.to_le_bytes());

    println!(".. fix_header_block_archivelog end");

    Ok(())
}