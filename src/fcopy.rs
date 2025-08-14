use rlimit::Resource;
use std::{env, process};
use log::debug;

#[allow(dead_code)]
#[path = "../src/oracle.rs"]
mod oracle;
#[allow(dead_code)]
#[path = "../src/inode.rs"]
mod inode;
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
    //
    // Require exactly three positional arguments: src_fname, dst_fname, conn_str
    //
    let mut args = env::args();
    let prog = args.next().unwrap_or_else(|| "fcopy".to_string());
    let src_fname_opt = args.next();
    let dst_fname_opt = args.next();
    let conn_str_opt = args.next();

    // Must always be all three given, and no extra args
    if src_fname_opt.is_none() || dst_fname_opt.is_none() || conn_str_opt.is_none() || args.next().is_some() {
        eprintln!("Usage: {prog} <src_fname> <dst_fname> <conn_str>");
        process::exit(2);
    }

    let src_fname = src_fname_opt.unwrap();
    let dst_fname = dst_fname_opt.unwrap();
    let conn_str = conn_str_opt.unwrap();

    //
    // make sure ulimit is set
    //
    const DEFAULT_SOFT_LIMIT: u64 = 1000 * 1024 * 1024;
    const DEFAULT_HARD_LIMIT: u64 = 1000 * 1024 * 1024;

    if let Err(err) = Resource::FSIZE.set(DEFAULT_SOFT_LIMIT, DEFAULT_HARD_LIMIT) {
        eprintln!("Failed to set file size limit (fsize): {err}");
        process::exit(-2);
    }

    // Establish a basic Oracle connection just to validate the string with existing helper
    // (kept for backward compatibility/diagnostics)
    let oracle = match OracleConnection::connect(Some(conn_str.clone())) {
        Ok(connection) => {
            println!("Oracle connection (wrapper) established.");
            connection
        },
        Err(e) => {
            eprintln!("Failed to connect to Oracle (wrapper): {}", e);
            process::exit(1);
        }
    };

    println!("DEBUG 1");
    let (filetype, filesize, blksize) = oracle.proc_getfilettr(&src_fname)?;
    println!("DEBUG 2");
    // Get DB unique name for the final argument by parsing src_fname (+DG/DB_UNIQUE_NAME/..)
    let _db_unique_name: String = match parse_db_unique_name(&src_fname) {
        Some(s) => s,
        None => {
            eprintln!("Failed to parse DB_UNIQUE_NAME from src_fname: expected '+DISKGROUP/DB_UNIQUE_NAME/...' but got: {}", src_fname);
            process::exit(2);
        }
    };
    debug!("db_unique_name is set to '{}'", _db_unique_name);
    println!("DEBUG 3");
    oracle._proc_copy(src_fname, filetype, blksize, filesize, dst_fname)?;
    println!("DEBUG 4");
    Ok(())
}