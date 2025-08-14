use rlimit::Resource;
use std::process;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    const DEFAULT_SOFT_LIMIT: u64 = 4 * 1024 * 1024;
    const DEFAULT_HARD_LIMIT: u64 = 4 * 1024 * 1024;

    if let Err(err) = Resource::FSIZE.set(DEFAULT_SOFT_LIMIT, DEFAULT_HARD_LIMIT) {
        eprintln!("Failed to set file size limit (fsize): {err}");
        process::exit(-2);
    }

    println!("File size limit set to 4MB!");

    Ok(())
}