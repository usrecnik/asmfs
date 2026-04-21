mod oracle;
mod fuse;
mod inode;
mod afd;

use clap::{Arg, ArgAction, Command};
use fuser::MountOption;
use fuser::SessionACL;
use fuser::Config;
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
            Arg::new("no-magic")
                .long("no-magic")
                .action(ArgAction::SetTrue)
                .help("Do not change magic bytes in first block of files (default: do change magic bytes)")
        )
        .arg(
            Arg::new("mirror")
                .long("mirror")
                .default_value("0")
                .help("0=>primary copy, 1=>first redundant copy, 2=>second redundant copy"),
        )
        .arg(
            Arg::new("threads")
                .long("threads")
                .default_value("8")
                .help("Number of threads for fuse operations (default: 8)"),
        )
        .arg(
            Arg::new("auto-unmount")
                .long("auto-unmount")
                .action(ArgAction::SetTrue)
                .help("Automatically unmount on process exit"),
        )
        .get_matches();

    let connection_string = matches.get_one::<String>("conn");
    let mountpoint = matches.get_one::<String>("MOUNT_POINT").unwrap();
    let use_raw = !matches.get_flag("no-raw");
    let magic = !matches.get_flag("no-magic");
    let mirror = matches.get_one::<String>("mirror").map(|s| s.as_str()).unwrap_or("0");
    let mirror: u8 = mirror.parse().unwrap_or(0);
    let threads = matches.get_one::<String>("threads").unwrap();
    let threads: usize = threads.parse().unwrap_or(8);

    let mut options = vec![MountOption::RO, MountOption::FSName("asmfs".to_string())];
    if matches.get_flag("auto-unmount") {
        options.push(MountOption::AutoUnmount);
    }

    options.push(MountOption::CUSTOM("max_read=33554432".into())); // 32MB max read
    options.push(MountOption::RO); // force read-only
    options.push(MountOption::Async);

    let mut cfg = Config::default();
    cfg.acl = SessionACL::Owner;
    cfg.n_threads = Some(threads);
    cfg.clone_fd = true;
    cfg.mount_options = options;

    let asmfs = AsmFS::new(mountpoint.clone(), connection_string.cloned(), use_raw, magic, mirror);

    match fuser::mount2(asmfs, mountpoint, &cfg) {
        Ok(_) => {},
        Err(e) => {
            eprintln!("Failed to mount FUSE filesystem: {:?}", e);
            eprintln!("Error kind: {:?}", e.kind());
            eprintln!("OS error code: {:?}", e.raw_os_error());
            std::process::exit(1);
        }
    }

}
