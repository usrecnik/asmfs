

use rlimit::Resource;
use std::{env, process};

#[path = "oracle.rs"]
mod oracle;
use crate::oracle::OracleConnection;

fn parse_db_unique_name(src_path: &str) -> Option<String> {
    // Example src_path: +DATA/DB_UNIQUE_NAME/whatever/path
    // We need the second component (index 1) when splitting by '/'.
    // Ignore any leading '+' on the very first component.
    let mut parts = src_path.split('/').filter(|s| !s.is_empty());
    // First component is diskgroup like "+DATA" (possibly without '+')
    let _diskgroup = parts.next()?;
    // Second component should be DB_UNIQUE_NAME
    parts.next().map(|s| s.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Require exactly three positional arguments: src_fname, dst_fname, conn_str
    let mut args = env::args();
    let prog = args.next().unwrap_or_else(|| "fcopy".to_string());
    let src_fname_opt = args.next();
    let dst_fname_opt = args.next();
    let conn_str_opt = args.next();
/*
    // Must always be all three given, and no extra args
    if src_fname_opt.is_none() || dst_fname_opt.is_none() || conn_str_opt.is_none() || args.next().is_some() {
        eprintln!("Usage: {prog} <src_fname> <dst_fname> <conn_str>");
        process::exit(2);
    }

    // Use source and target file names (remove underscore in the variable name)
    let src_fname = src_fname_opt.unwrap();
    let dst_fname = dst_fname_opt.unwrap();
    let conn_str = conn_str_opt.unwrap();

    const DEFAULT_SOFT_LIMIT: u64 = 4 * 1024 * 1024;
    const DEFAULT_HARD_LIMIT: u64 = 4 * 1024 * 1024;

    if let Err(err) = Resource::FSIZE.set(DEFAULT_SOFT_LIMIT, DEFAULT_HARD_LIMIT) {
        eprintln!("Failed to set file size limit (fsize): {err}");
        process::exit(-2);
    }

    // Establish a basic Oracle connection just to validate the string with existing helper
    // (kept for backward compatibility/diagnostics)
    match OracleConnection::connect(Some(conn_str.clone())) {
        Ok(_) => println!("Oracle connection (wrapper) established."),
        Err(e) => {
            eprintln!("Failed to connect to Oracle (wrapper): {}", e);
            process::exit(1);
        }
    }

    // Establish a direct Oracle connection to execute PL/SQL procedures
    let (user, pass, inst) = match conn_str.split_once('@') {
        Some((user_pass, after_at)) => match user_pass.split_once('/') {
            Some((u, p)) => (u.to_string(), p.to_string(), after_at.to_string()),
            None => {
                eprintln!("Invalid conn_str format: expected user/pass@inst");
                process::exit(1);
            }
        },
        None => {
            eprintln!("Invalid conn_str format: expected user/pass@inst");
            process::exit(1);
        }
    };

    let conn = Connector::new(&user, &pass, &inst)
        .privilege(Privilege::Sysdba)
        .connect()?;

    // Retrieve source file attributes needed by dbms_diskgroup.copy
    let mut stmt = conn.statement("begin dbms_diskgroup.getfileattr(:b_target, :b_filetype, :b_filesize, :b_blksize); end;").build()?;
    stmt.execute(&[&src_fname, &OracleType::Int64, &OracleType::Int64, &OracleType::Int64])?;
    let src_ftyp: u32 = stmt.bind_value(2)?;
    let src_fsiz: u64 = stmt.bind_value(3)?;
    let src_blksz: u32 = stmt.bind_value(4)?; // logical block size

    println!("dbms_diskgroup.getfileattr: src_path={}, src_ftyp={}, src_fsiz={}, src_blksz={}", src_fname, src_ftyp, src_fsiz, src_blksz);

    // Get DB unique name for the final argument by parsing src_fname (+DG/DB_UNIQUE_NAME/..)
    let dbuniquename: String = match parse_db_unique_name(&src_fname) {
        Some(s) => s,
        None => {
            eprintln!("Failed to parse DB_UNIQUE_NAME from src_fname: expected '+DISKGROUP/DB_UNIQUE_NAME/...' but got: {}", src_fname);
            process::exit(2);
        }
    };

    let sparse_option: i64 = 0; // default sparse option = off

    // Execute copy. First three params are :connect_iden, :usrname, :passwd, 
    // other not given params are also  :connect_iden, :usrname, :passwd (but for dest). 
    let mut cstmt = conn
        .statement(
            "begin dbms_diskgroup.copy('', '', '', :src_path, :src_ftyp, :src_blksz, :src_fsiz, '', '', '', :dst_path, 1, 0, :sparse_option, 0, '', :dbuniquename); end;"
        )
        .build()?;

    cstmt.execute(&[
        &src_fname,
        &src_ftyp,
        &src_blksz,
        &src_fsiz,
        &dst_fname,
        &sparse_option,
        &dbuniquename,
    ])?;

    println!("dbms_diskgroup.copy submitted: {} -> {} (dbuniquename={})", src_fname, dst_fname, dbuniquename);

    println!("File size limit set to 4MB!");
*/
    Ok(())
}
