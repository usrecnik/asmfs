use rlimit::Resource;
use std::{env, process};
use crate::oracle::OracleConnection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Require exactly three positional arguments: src_fname, dst_fname, conn_str
    let mut args = env::args();
    let prog = args.next().unwrap_or_else(|| "fcopy".to_string());
    let src_fname = args.next();
    let dst_fname = args.next();
    let conn_str = args.next();

    // Must always be all three given, and no extra args
    if src_fname.is_none() || dst_fname.is_none() || conn_str.is_none() || args.next().is_some() {
        eprintln!("Usage: {prog} <src_fname> <dst_fname> <conn_str>");
        process::exit(2);
    }

    // Keep src and dst as underscore-prefixed for now to avoid unused warnings
    let _src_fname = src_fname.unwrap();
    let _dst_fname = dst_fname.unwrap();
    // Remove underscore and use conn_str to establish Oracle connection
    let conn_str = conn_str.unwrap();

    const DEFAULT_SOFT_LIMIT: u64 = 4 * 1024 * 1024;
    const DEFAULT_HARD_LIMIT: u64 = 4 * 1024 * 1024;

    if let Err(err) = Resource::FSIZE.set(DEFAULT_SOFT_LIMIT, DEFAULT_HARD_LIMIT) {
        eprintln!("Failed to set file size limit (fsize): {err}");
        process::exit(-2);
    }

    // Open connection to Oracle using the provided connection string
    match OracleConnection::connect(Some(conn_str)) {
        Ok(_) => println!("Oracle connection established."),
        Err(e) => {
            eprintln!("Failed to connect to Oracle: {}", e);
            process::exit(1);
        }
    }

    println!("File size limit set to 4MB!");

    Ok(())
}