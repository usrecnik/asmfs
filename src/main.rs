mod oracle;
mod fuse;
mod inode;
mod afd;

use clap::{Arg, ArgAction, Command};
use fuser::MountOption;
use fuse::AsmFS;

fn main() {
    env_logger::init();

    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author("Urh Srecnik")
        .arg(
            Arg::new("MOUNT_POINT")
                .required(true)
                .index(1)
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("conn")
                .long("conn")
                .value_name("CONNECTION_STRING")
                .help("Connection string to remote ASM instance - user/pass@host:port/service (user must have sysdba)")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("no-raw")
                .long("no-raw")
                .action(ArgAction::SetTrue)
                .help("Use DBMS_DISKGROUP.READ() instead of raw device access")
        )
        .arg(
            Arg::new("mirror")
                .long("mirror")
                .default_value("0")
                .help("0=>primary copy, 1=>first redundant copy, 2=>second redundant copy"),
        )
        .arg(
            Arg::new("auto-unmount")
                .long("auto-unmount")
                .action(ArgAction::SetTrue)
                .help("Automatically unmount on process exit"),
        )
        .arg(
            Arg::new("allow-root")
                .long("allow-root")
                .action(ArgAction::SetTrue)
                .help("Allow root user to access filesystem"),
        )
        .get_matches();

    let connection_string = matches.get_one::<String>("conn");
    let mountpoint = matches.get_one::<String>("MOUNT_POINT").unwrap();
    let use_raw = !matches.get_flag("no-raw");
    let mirror = matches.get_one::<String>("mirror").map(|s| s.as_str()).unwrap_or("0");
    let mirror: u8 = mirror.parse().unwrap_or(0);

    let mut options = vec![MountOption::RO, MountOption::FSName("asmfs".to_string())];
    if matches.get_flag("auto-unmount") {
        options.push(MountOption::AutoUnmount);
    }
    if matches.get_flag("allow-root") {
        options.push(MountOption::AllowRoot);
    }
    
    match fuser::mount2(AsmFS::new(mountpoint.clone(), connection_string.cloned(), use_raw, mirror), mountpoint, &options) {
        Ok(_) => {},
        Err(e) => {
            eprintln!("Failed to mount FUSE filesystem: {:?}", e);
            eprintln!("Error kind: {:?}", e.kind());
            eprintln!("OS error code: {:?}", e.raw_os_error());
            std::process::exit(1);
        }
    }
}
