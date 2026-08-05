mod oraenv;
mod oracle;
mod fuse;
mod inode;
mod afd;

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::FromRawFd;
use clap::{Arg, ArgAction, Command};
use fuser::MountOption;
use fuser::SessionACL;
use fuser::Config;
use fuse::AsmFS;
use crate::oraenv::bootstrap_oracle_env;

fn main() {
    bootstrap_oracle_env(
        &std::env::args_os().collect::<Vec<_>>(),
    );

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
            Arg::new("daemon")
                .long("daemon")
                .action(ArgAction::SetTrue)
                .help("Mount in the background"),
        )
        .arg(
            Arg::new("auto-unmount")
                .long("auto-unmount")
                .action(ArgAction::SetTrue)
                .help("Automatically unmount on process exit"),
        )
        .get_matches();

    let connection_string = matches.get_one::<String>("conn");
    let mountpoint_arg = matches.get_one::<String>("MOUNT_POINT").unwrap();
    let use_raw = !matches.get_flag("no-raw");
    let magic = !matches.get_flag("no-magic");
    let mirror = matches.get_one::<String>("mirror").map(|s| s.as_str()).unwrap_or("0");
    let mirror: u8 = mirror.parse().unwrap_or(0);
    let threads = matches.get_one::<String>("threads").unwrap();
    let threads: usize = threads.parse().unwrap_or(8);
    let daemon = matches.get_flag("daemon");

    let mountpoint = match std::fs::canonicalize(mountpoint_arg) {
        Ok(path) => path,
        Err(e) => {
            eprintln!("Failed to resolve mountpoint '{}': {e}", mountpoint_arg);
            std::process::exit(1);
        }
    };

    let mountpoint_string = match mountpoint.to_str() {
        Some(path) => path.to_owned(),
        None => {
            eprintln!("Mountpoint is not valid UTF-8: {}", mountpoint.display());
            std::process::exit(1);
        }
    };

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

    let mut status_pipe = start_daemon(daemon);

    let asmfs = match AsmFS::new(mountpoint_string, connection_string.cloned(), use_raw, magic, mirror) {
        Ok(asmfs) => asmfs,
        Err(e) => startup_failed(&mut status_pipe, &e)
    };

    let session = match fuser::Session::new(asmfs, &mountpoint, &cfg) {
        Ok(session) => session,
        Err(e) => startup_failed(&mut status_pipe, &format!("Failed to mount FUSE filesystem: {e}"))
    };

    let background = match session.spawn() {
        Ok(background) => background,
        Err(e) => startup_failed(&mut status_pipe, &format!("Failed to start FUSE workers: {e}"))
    };

    if let Some(mut pipe) = status_pipe.take() {
        // is block only runs in the daemon child because foreground mode has status_pipe == None

        if let Err(e) = pipe.write_all(b"OK\n").and_then(|_| pipe.flush()) {
            eprintln!("Failed to report daemon startup: {e}");
            std::process::exit(1);
        }

        // Close the pipe so the parent receives EOF and exits.
        drop(pipe);

        if let Err(e) = redirect_stdio_to_devnull() {
            eprintln!("Failed to redirect daemon standard streams: {e}");
            std::process::exit(1);
        }
    }

    if let Err(e) = background.join() {
        eprintln!("FUSE session failed: {e}");
        std::process::exit(1);
    }

}
/*
--daemon needs two processes:
  - The parent waits only long enough to learn whether mounting succeeded, then exits.
  - The child performs the mount and stays alive to serve the filesystem.

*/
fn start_daemon(enabled: bool) -> Option<File> {
    if !enabled {
        return None;
    }

    let mut fds = [0 as libc::c_int; 2];

    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        eprintln!(
            "Failed to create daemon status pipe: {}",
            std::io::Error::last_os_error()
        );
        std::process::exit(1);
    }

    match unsafe { libc::fork() } {
        -1 => {
            let error = std::io::Error::last_os_error();

            unsafe {
                libc::close(fds[0]);
                libc::close(fds[1]);
            }

            eprintln!("Failed to fork daemon: {error}");
            std::process::exit(1);
        }

        0 => {
            // Child keeps only the write end.
            unsafe {
                libc::close(fds[0]);
            }

            if unsafe { libc::setsid() } == -1 {
                let error = std::io::Error::last_os_error();

                // SAFETY: the child exclusively owns this descriptor.
                let mut pipe = unsafe { File::from_raw_fd(fds[1]) };
                let _ = writeln!(pipe, "Failed to create daemon session: {error}");
                std::process::exit(1);
            }

            // SAFETY: the child exclusively owns this descriptor.
            Some(unsafe { File::from_raw_fd(fds[1]) })
        }

        _ => {
            // Parent keeps only the read end.
            unsafe {
                libc::close(fds[1]);
            }

            // SAFETY: the parent exclusively owns this descriptor.
            let mut pipe = unsafe { File::from_raw_fd(fds[0]) };
            let mut status = String::new();

            match pipe.read_to_string(&mut status) {
                Ok(_) if status.starts_with("OK\n") => {
                    std::process::exit(0);
                }
                Ok(_) if status.is_empty() => {
                    eprintln!("asmfs: daemon child exited before reporting");
                    std::process::exit(1);
                }
                Ok(_) => {
                    eprint!("{status}");
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("asmfs: failed to read daemon status: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
}

fn startup_failed(status_pipe: &mut Option<File>, message: &str) -> ! {
    if let Some(pipe) = status_pipe.as_mut() {
        let _ = writeln!(pipe, "{message}");
        let _ = pipe.flush();
    } else {
        eprintln!("{message}");
    }

    std::process::exit(1);
}

// later we should probably redirect to file-based logging instead:
fn redirect_stdio_to_devnull() -> std::io::Result<()> {
    let devnull = unsafe {
        libc::open(
            c"/dev/null".as_ptr(),
            libc::O_RDWR | libc::O_CLOEXEC,
        )
    };

    if devnull == -1 {
        return Err(std::io::Error::last_os_error());
    }

    for target_fd in [0, 1, 2] {
        if unsafe { libc::dup2(devnull, target_fd) } == -1 {
            let error = std::io::Error::last_os_error();

            unsafe {
                libc::close(devnull);
            }

            return Err(error);
        }
    }

    if devnull > 2 {
        unsafe {
            libc::close(devnull);
        }
    }

    Ok(())
}
