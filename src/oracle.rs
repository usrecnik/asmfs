use std::time::{SystemTime, UNIX_EPOCH};
use oracle::{Connection, Connector, Error, ErrorKind, Privilege, Row, ResultSet};
use fuser::{FileType, FileAttr};
use oracle::sql_type::{OracleType, Timestamp};
use chrono::{NaiveDate, DateTime, Utc};
use crate::inode;
use inode::Inode;
use log::{debug, error, info}; // debug, info, error

//use log::{info}; // debug, error

pub struct OracleConnection {
    conn: Connection
}

const ASM_ALIAS_COLUMNS: &str = "a.reference_index, a.alias_index, a.file_number, a.name, a.alias_directory, a.system_created";
const ASM_FILE_COLUMNS: &str = "f.bytes, f.blocks, f.creation_date, f.modification_date";
const FILE_TYPE_ARCHIVELOG: u32 = 4;

struct AsmAlias {
    reference_index: u32,                   // v$asm_alias.reference_index (contains group_number in high-order 8 bits), use get_inode.get_group_number
    alias_index: u32,                       // v$asm_alias.alias_index
    file_number: u32,                       // v$asm_alias.file_number
    name: String,                           // v$asm_alias.name
    alias_directory: String,                // v$asm_alias.alias_directory ("Y" | "N")
    system_created: String,                 // v$asm_alias.system_created ("Y" | "N")
    bytes: Option<u64>,                     // v$asm_file.bytes
    blocks: Option<u64>,                    // v$asm_file.blocks
    creation_date: Option<Timestamp>,       // v$asm_file.creation_date
    modification_date: Option<Timestamp>,   // v$asm_file.modification_date
}

impl AsmAlias {

    pub fn from_row_file(row: &Row) -> Result<Self, Error> {
        Ok(Self {
            reference_index: row.get("REFERENCE_INDEX")?,
            alias_index: row.get("ALIAS_INDEX")?,
            file_number: row.get("FILE_NUMBER")?,
            name: row.get("NAME")?,
            alias_directory: row.get("ALIAS_DIRECTORY")?,
            system_created: row.get("SYSTEM_CREATED")?,
            bytes: row.get("BYTES")?,
            blocks: row.get("BLOCKS")?,
            creation_date: row.get("CREATION_DATE")?,
            modification_date: row.get("MODIFICATION_DATE")?,
        })
    }

    pub fn from_row_alias(row: &Row) -> Result<Self, Error> {
        Ok(Self {
            reference_index: row.get("REFERENCE_INDEX")?,
            alias_index: row.get("ALIAS_INDEX")?,
            file_number: row.get("FILE_NUMBER")?,
            name: row.get("NAME")?,
            alias_directory: row.get("ALIAS_DIRECTORY")?,
            system_created: row.get("SYSTEM_CREATED")?,
            bytes: Option::None,
            blocks: Option::None,
            creation_date: Option::None,
            modification_date: Option::None
        })
    }

    pub fn get_inode(&self) -> Inode {
        Inode::from_alias(self.reference_index, self.alias_index)
    }

    fn get_ftype(&self) -> FileType {
        if self.alias_directory == "Y" {
            FileType::Directory
        } else {
            if self.system_created == "Y" {
                FileType::RegularFile
            } else {
                FileType::Symlink
            }
        }
    }

    fn get_creation_date(&self) -> SystemTime {
        if self.creation_date.is_none() {
            SystemTime::UNIX_EPOCH
        } else {
            oracle_timestamp_to_system_time(&self.creation_date.unwrap())
        }
    }

    fn get_modification_date(&self) -> SystemTime {
        if self.modification_date.is_none() {
            SystemTime::UNIX_EPOCH
        } else {
            oracle_timestamp_to_system_time(&self.modification_date.unwrap())
        }
    }

    pub fn get_file_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.get_inode().get_ino(),
            size: self.bytes.unwrap_or(0),
            blocks: self.blocks.unwrap_or(0),   // @todo: this is probably not size in oracle blocks
            atime: UNIX_EPOCH,
            mtime: self.get_modification_date(),
            ctime: self.get_creation_date(),
            crtime: UNIX_EPOCH,
            kind: self.get_ftype(),
            perm: 0o755,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

}

impl OracleConnection {

    pub fn connect(conn_str: Option<String>) -> Result<Self, Error> {
        if conn_str.is_none() {
            let conn = Connector::new("", "", "")
                .external_auth(true)
                .privilege(Privilege::Sysasm)
                .connect()?;

            return Ok(Self{conn});
        } else {
            let str = conn_str.unwrap();

            let (user, pass, inst) = match str.split_once('@') {
                Some((user_pass, after_at)) => {
                    match user_pass.split_once('/') {
                        Some((u, p)) => (u, p, after_at),
                        None => {
                            eprintln!("Invalid format: missing '/' in user/pass");
                            std::process::exit(1);
                        }
                    }
                },
                None => {
                    eprintln!("Invalid format: missing '@'");
                    std::process::exit(1);
                }
            };

            let conn = Connector::new(user, pass, inst)
                .privilege(Privilege::Sysdba)
                .connect()?;

            return Ok(Self{conn});
        }
    }

    fn select_diskgroup_all(&self) -> Result<ResultSet<'_,Row>, Error> {
        let query = r#"
            select group_number, '+' || name as name from v$asm_diskgroup order by name
        "#;
        self.conn.query(query, &[])
    }

    fn select_diskgroup_by_name(&self, group_name: &str) -> Result<Row, Error> {
        let query = r#"
            select group_number, '+' || name as name from v$asm_diskgroup where name=:1
        "#;
        self.conn.query_row(query, &[&group_name])
    }

    fn select_alias_by_parent_index(&self, parent_index: u32) -> Result<ResultSet<'_,Row>, Error> {
        let query = format!(r#"
            select {}
                from v$asm_alias a
                where a.parent_index=:1
                order by a.name
        "#, ASM_ALIAS_COLUMNS);

        self.conn.query(query.as_str(), &[&parent_index])
    }

    fn select_alias_file_by_parent_index_and_name(&self, parent_index: u32, name: &str) -> Result<Row, Error> {
        let query = format!(r#"
            select {}, {}
                from v$asm_alias a
                left join v$asm_file f on f.file_number = a.file_number
                where a.parent_index = :1
                    and a.name = :2
        "#, ASM_ALIAS_COLUMNS, ASM_FILE_COLUMNS);

        self.conn.query_row(query.as_str(), &[&parent_index, &name])
    }

    pub fn select_alias_file_by_reference_index_and_alias_index(&self, reference_index: u32, alias_index: u32) -> Result<Row, Error> {
        let query = format!(r#"
            select {}, {}
                from v$asm_alias a
                left join v$asm_file f on f.file_number = a.file_number
                where a.reference_index = :1 and a.alias_index = :2
        "#, ASM_ALIAS_COLUMNS, ASM_FILE_COLUMNS);

        self.conn.query_row(query.as_str(), &[&reference_index, &alias_index])
    }

    pub fn query_asm_diskgroup_vec(&self) -> Result<Vec<(u64, FileType, String)>, Error> {
        let rs = self.select_diskgroup_all()?;
        let mut retval :Vec<(u64, FileType, String)> = Vec::new();
        retval.push((1, FileType::Directory, ".".to_string()));
        retval.push((1, FileType::Directory, "..".to_string()));
        for r in rs {
            let row = r?;
            let group_number: u8 = row.get(0)?;
            let name: String = row.get(1)?;

            let inode = Inode::from_group_number(group_number);
            // inode.debug_dump();
            retval.push((inode.get_ino(), FileType::Directory, name));
        }
        Ok(retval)
    }

    pub fn query_asm_diskgroup_ent_name(&self, name: &str) -> Result<FileAttr, Error> {
        let dg_name = name.replace("+", "");
        let row = self.select_diskgroup_by_name(dg_name.as_str())?;

        let group_number: u8 = row.get(0)?;
        let inode = Inode::from_group_number(group_number);

        Ok(FileAttr {
            ino: inode.get_ino(),
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 512,
        })
    }

    pub fn query_asm_diskgroup_ent_ino(&mut self, ino: u64) -> FileAttr {
        FileAttr {
            ino: ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    // all aliases in a given folder
    pub fn query_asm_alias_vec(&self, ino: u64) -> Result<Vec<(u64, FileType, String)>, Error> {
        let inode = Inode::from_ino(ino);
        let parent_index = inode.get_reference_index();
        let rs = self.select_alias_by_parent_index(parent_index)?;

        let mut retval :Vec<(u64, FileType, String)> = Vec::new();
        for r in rs {
            let row = r?;
            let alias = AsmAlias::from_row_alias(&row)?;
            retval.push((alias.get_inode().get_ino(), alias.get_ftype(), alias.name));
        }

        Ok(retval)
    }

    pub fn query_asm_alias_ent(&self, parent_ino: u64, name: &str) -> Result<FileAttr, Error> {
        let parent_inode = Inode::from_ino(parent_ino);
        let row = self.select_alias_file_by_parent_index_and_name(parent_inode.get_reference_index(), name)?;

        let alias = AsmAlias::from_row_file(&row)?;
        // @todo: if archivelog add +1 to blocks (filesize) for trailer block, see v$asm_file.type
        Ok(alias.get_file_attr())
    }

    pub fn query_asm_alias_ent_ino(&self, ino: u64) -> Result<FileAttr, Error> {
        let inode = Inode::from_ino(ino);
        let row = self.select_alias_file_by_reference_index_and_alias_index(inode.get_reference_index(), inode.get_alias_index())?;

        let alias = AsmAlias::from_row_file(&row)?;
        Ok(alias.get_file_attr())
    }

    pub fn query_asm_alias_link(&self, ino: u64) -> Result<String, Error> {
        let link_inode = Inode::from_ino(ino);
        let link_row = self.select_alias_file_by_reference_index_and_alias_index(link_inode.get_reference_index(), link_inode.get_alias_index())?;
        let link_struct = AsmAlias::from_row_file(&link_row)?;

        let query = r#"
            select x.* from (
                select reference_index, alias_index, file_number, alias_directory, system_created,
                       concat('+' || group_name, sys_connect_by_path(name, '/')) as name
                    from (
                        select a.*, g.name as group_name
                            from v$asm_alias a
                            join v$asm_diskgroup g on a.group_number = g.group_number
                    )
                    start with (mod(parent_index, power(2, 24))) = 0
                    connect by prior reference_index = parent_index
            ) x where x.file_number = :1 and x.system_created = 'Y'
            fetch first 1 rows only
        "#;

        let target_row = self.conn.query_row(query, &[&link_struct.file_number])?;
        let target_name :String = target_row.get("NAME")?;
        Ok(target_name)
    }

    // filetype, filesize_ora, filesize_fs, blksize
    pub fn proc_getfilettr(&self, target_path: &String) -> Result<(u32, u64, u64, u32), Error> {
        let mut stmt = self.conn.statement("begin dbms_diskgroup.getfileattr(:b_target, :b_filetype, :b_filesize, :b_blksize); end;").build()?;
        stmt.execute(&[target_path, &OracleType::Int64, &OracleType::Int64, &OracleType::Int64])?;
        let filetype: u32 = stmt.bind_value(2)?;
        let filesize: u64 = stmt.bind_value(3)?;
        let blksize: u32 = stmt.bind_value(4)?; // logical block size
        debug!(".. dbms_diskgroup.getfileattr(): target={}, filetype={}, filesize={}, blksize={}", target_path, filetype, filesize, blksize);

        if filetype == FILE_TYPE_ARCHIVELOG {
            let filesize_fs = filesize + (blksize as u64);
            debug!(".... filesize_fs raised for one block to {} to accommodate the trailer", filesize);
            Ok((filetype, filesize, filesize_fs, blksize))
        } else {
            Ok((filetype, filesize, filesize, blksize))
        }
    }

    pub fn _proc_copy(&self, src_fname: String, src_filetype: u32, src_blksize: u32, src_filesize: u64, dst_fname: String) -> Result<(), Error> {
        let sql = "begin dbms_diskgroup.copy('', '', '', :src_path, :src_ftyp, :src_blksz, :src_fsiz, '', '', '', :dst_path, 1, 0, 0, 0, '', ''); end;";
        let mut stmt = self.conn.statement(sql).build()?;

        info!("Calling dbms_diskgroup.copy now...");
        stmt.execute(&[
            &src_fname,
            &src_filetype,
            &src_blksize,
            &src_filesize,
            &dst_fname
        ])?;
        info!("Calling dbms_diskgroup.copy completed.");

        Ok(())
    }

    pub fn proc_open(&self, ino: u64) -> Result<(u64, u32, u64, u32), Error> {
        let target_path = self.query_asm_alias_link(ino)?;

        let (filetype, filesize, _filesize_fs, blksize) = self.proc_getfilettr(&target_path)?;

        let mut stmt = self.conn.statement("begin dbms_diskgroup.open(:b_target, :b_mode, :b_filetype, :b_blksize, :b_handle, :b_pblksize, :b_filesize); end;").build()?;
        stmt.execute(&[&target_path, &"r", &filetype, &blksize, &OracleType::Int64, &OracleType::Int64, &filesize])?;

        let handle: u64 = stmt.bind_value(5)?;
        let _pblksize: u64 = stmt.bind_value(6)?;   // physical block size

        debug!(".. dbms_diskgroup.open(): handle={}, pblksize={}, target={}, filetype={}, filesize={}, blksize={}", handle, _pblksize, target_path, filetype, filesize, blksize);

        Ok((handle, blksize, filesize, filetype))
    }

    pub fn proc_close(&self, fd: u64) -> Result<(), Error> {
        let mut stmt = self.conn.statement("begin dbms_diskgroup.close(:b_handle); end;").build()?;
        stmt.execute(&[&fd])?;
        Ok(())
    }

    fn proc_read_int(&self, handle: u64, block_size: u32, offset_in_blocks: i64, amount_in_blocks: u32) -> Result<Vec<u8>, Error> {
        let mut stmt = self.conn.statement("begin dbms_diskgroup.read(:b_handle, :b_offset, :b_length, :b_buffer); end;").build()?;
        let mut amount_in_bytes = block_size * amount_in_blocks;
        let _amount_in_bytes = block_size * amount_in_blocks;

        println!(".... dbms_diskgroup.read params: handle={}, offset_in_blocks={}, amount_in_blocks={}, amount_in_bytes={}", handle, offset_in_blocks, amount_in_blocks, amount_in_bytes);

        stmt.execute(&[
            &handle,                              // IN
            &offset_in_blocks,                    // IN
            &mut amount_in_bytes,                 // IN OUT
            &OracleType::Raw(_amount_in_bytes),   // OUT
        ])?;

        let mut buffer: Vec<u8> = stmt.bind_value(4)?;

        /*
        println!("---- first 64k buffer dump (amount_in_bytes={}) -----", amount_in_bytes);
        for (_i, byte) in buffer.iter().take(64).enumerate() {
            print!("{:02X} ", byte);
            if (_i+1) % 8 == 0 {
                print!(" | ");
            }
            if (_i+1) % 16 == 0 {
                println!(" ");
            }
        }
        println!(" ");
        */

        buffer.truncate(amount_in_bytes as usize);
        Ok(buffer)
    }

    pub fn proc_read(&self, fh: u64, offset_in_bytes: i64, mut requested_bytes: u32, block_size: u32, size_in_bytes: u64, file_type: u32) -> Result<Vec<u8>, Error> {

        // some files seem to start at index zero, and some seem to start with the first block being 1 instead of 0.
        let fix: i64 = match file_type {
            13 => 1, // spfile
            _ => 0 // (2 => datafile), careful, the first block is two bytes different than asmcmd cp; code is not tested with fix=1
        };
        println!(".. offset fix is {}", fix);

        if block_size >= 32*1024 {
            error!("Reading files with 32k block size is not supported. Returning empty buffer!");
            return Ok(Vec::<u8>::new());
        }

        if requested_bytes as u64 > size_in_bytes {  // if file size is, say 3K and requested length is 4K, we can only really request 3K.
            requested_bytes = size_in_bytes as u32;
            debug!(".. proc_read, length was > than bytes_size, thus lowered to {}", requested_bytes);
        }

        let size_in_blocks :i64 = (size_in_bytes / block_size as u64) as i64; // includes trailer block (e.g. for archivelog)
        println!(".. size_in_blocks={} (size_in_bytes={} / block_size={})", size_in_blocks, size_in_bytes, block_size);
        let size_in_blocks_raw :i64; // does *NOT* include trailer block (e.g. for archivelog)
        if file_type == FILE_TYPE_ARCHIVELOG {
            size_in_blocks_raw = size_in_blocks - 1;
        } else {
            size_in_blocks_raw = size_in_blocks;
        }

        let offset_in_blocks :i64 = offset_in_bytes / block_size as i64;
        println!(".. size_in_blocks={}, size_in_blocks_raw={}, offset_in_blocks={}, size_in_bytes={}, file_type={}", size_in_blocks, size_in_blocks_raw, offset_in_blocks, size_in_bytes, file_type);
        if offset_in_blocks > size_in_blocks {
            println!(".. **offset in blocks bigger than file size in blocks, returning empty buffer");
            return Ok(Vec::<u8>::new());
        }

        let requested_blocks = (requested_bytes as i64 + block_size as i64 - 1) / block_size as i64;  // number of blocks to read
        println!(".. requested_blocks={}", requested_blocks);

        let read_step_blocks =
            if offset_in_blocks == 0 {
                1 // when reading header block, we can only read one block at a time
            } else {
                (24 * 1024) / block_size
            };

        println!(".. read_step_blocks={}", read_step_blocks);
        let mut buffer: Vec<u8> = Vec::with_capacity(requested_bytes as usize);

        // we can read at most 24K at a time (RAW(32767) which is one byte less than 32K, which is the limit)
        let mut already_read_blocks = 0;

        for i in (offset_in_blocks .. offset_in_blocks + (requested_blocks-fix)).step_by(read_step_blocks as usize) {
            let offset_in_blocks = i + fix;
            let mut amount_in_blocks = read_step_blocks;

            if already_read_blocks + read_step_blocks > requested_blocks as u32 {
                //amount_in_blocks = (already_read_blocks + read_step_blocks) - requested_blocks as u32 - 1; // -1 because blocks are zero-based (not fix)
                // println!(".... amount_in_blocks reduced to {} ((already_read_blocks={} + read_step_blocks={}) - requested_blocks={})", amount_in_blocks, already_read_blocks, read_step_blocks, requested_blocks);
                amount_in_blocks = requested_blocks as u32 - already_read_blocks;
                println!(".... a) amount_in_blocks={} (requested_blocks={} - already_read_blocks={})", amount_in_blocks, requested_blocks, already_read_blocks);
            }

            if offset_in_blocks + amount_in_blocks as i64 >= size_in_blocks_raw { // >= because if file_size=640 then we need to (can) read block 640.
                amount_in_blocks = (size_in_blocks - (offset_in_blocks - fix)) as u32;
                println!(".... b) amount_in_blocks={} (size_in_blocks={} - (offset_in_blocks={} - fix={}))", amount_in_blocks, size_in_blocks, offset_in_blocks, fix);
            }

            let tmp_vec :Vec<u8> = self.proc_read_int(fh, block_size, offset_in_blocks, amount_in_blocks)?;
            already_read_blocks += amount_in_blocks;

            buffer.extend(tmp_vec);
        }

        // add trailer block (e.g. for archivelog)
        if file_type == FILE_TYPE_ARCHIVELOG {
            println!("### this is archivelog, already_read_blocks={}, requested_blocks={}, a<b={}", already_read_blocks, requested_blocks, already_read_blocks < requested_blocks as u32);
            if already_read_blocks < requested_blocks as u32 {
                println!("### offset_in_blocks={} + already_read_blocks={} = {} ==?== size_in_blocks_raw={}", offset_in_blocks, already_read_blocks, offset_in_blocks+already_read_blocks as i64, size_in_blocks_raw);
                if offset_in_blocks + already_read_blocks as i64 == size_in_blocks_raw {
                    println!("### adding trailer block ***********************************");
                    let trail_vec :Vec<u8> = vec![0xFE; 512];
                    buffer.extend(trail_vec);
                }
            }
        }

        // convert headers to local filesystem
        if (offset_in_blocks == 0) && (file_type == FILE_TYPE_ARCHIVELOG) {
            self.fix_header_block_archivelog(&mut buffer)?;
        }

        println!(".. done, read {} blocks (=already_read_blocks).", already_read_blocks);
        buffer.truncate(requested_bytes as usize);

        Ok(buffer)
    }

    fn fix_header_block_archivelog(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
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

}

fn oracle_timestamp_to_system_time(ts: &Timestamp) -> SystemTime {
    let nd = NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day()).unwrap().and_hms_opt(ts.hour(), ts.minute(), ts.second()).unwrap();
    let datetime_utc: DateTime<Utc> = DateTime::<Utc>::from_naive_utc_and_offset(nd, Utc);
    SystemTime::from(datetime_utc)
}