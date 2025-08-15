mod oracle;
mod fuse;
mod inode;

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
    let mut options = vec![MountOption::RO, MountOption::FSName("asmfs".to_string())];
    if matches.get_flag("auto-unmount") {
        options.push(MountOption::AutoUnmount);
    }
    if matches.get_flag("allow-root") {
        options.push(MountOption::AllowRoot);
    }
    
    fuser::mount2(AsmFS::new(mountpoint.clone(), connection_string.cloned()), mountpoint, &options).unwrap();
}
