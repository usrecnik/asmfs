mod oraenv;
mod oracle;
mod fuse;
mod inode;
mod afd;

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::Path;
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
            Arg::new("PATH_ARGS")
                .required(true)
                .index(1)
                .num_args(1..=2)
                .help("Mount FUSE using <MOUNTPOINT> or <SPEC> <MOUNTPOINT>"), // SPEC is ignored, such syntax is supported only because of fstab compatibility.
        )
        .arg(
            Arg::new("mount-options")
                .short('o')
                .value_name("OPTIONS")
                .action(ArgAction::Append)
                .help("Comma-separated mount options"),
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
            Arg::new("log-file")
                .long("log-file")
                .value_name("PATH")
                .requires("daemon")
                .help("Write daemon stdout and stderr to this file"),
        )
        .arg(
            Arg::new("auto-unmount")
                .long("auto-unmount")
                .action(ArgAction::SetTrue)
                .help("Automatically unmount on process exit"),
        )
        // mount(8) may pass following standard external-helper flags. Most are handled
        // upstream by mount or mount.fuse3; accepting them prevents clap from
        // rejecting valid helper invocations. In this interface, -f means fake
        // (dry run), not the FUSE convention of foreground mode.
        .arg(
            Arg::new("helper-sloppy")
                .short('s')
                .action(ArgAction::SetTrue)
                .hide(true),
        )
        .arg(
            Arg::new("helper-no-mtab")
                .short('n')
                .action(ArgAction::SetTrue)
                .hide(true),
        )
        .arg(
            Arg::new("helper-verbose")
                .short('v')
                .action(ArgAction::SetTrue)
                .hide(true),
        )
        .arg(
            Arg::new("helper-namespace")
                .short('N')
                .value_name("NAMESPACE")
                .action(ArgAction::Set)
                .hide(true),
        )
        .arg(
            Arg::new("helper-type")
                .short('t')
                .value_name("TYPE")
                .action(ArgAction::Set)
                .hide(true),
        )
        .arg(
            Arg::new("fake")
                .short('f')
                .action(ArgAction::SetTrue)
                .help("Validate arguments without mounting"),
        )
        .get_matches();

    /*
     This produces from '-o ro,mirror=1 -o no-magic' something like:
          [
              ("ro", None),
              ("mirror", Some("1")),
              ("no-magic", None),
          ]
    */
    let mount_options: Vec<(&str, Option<&str>)> = matches
        .get_many::<String>("mount-options")
        .into_iter()
        .flatten()
        .flat_map(|options| options.split(','))
        .filter(|item| !item.is_empty())
        .map(|item| match item.split_once('=') {
            Some((key, value)) => (key, Some(value)),
            None => (item, None),
        })
        .collect();


    let mountpoint_arg = matches.get_many::<String>("PATH_ARGS").unwrap();
    let daemon = matches.get_flag("daemon") || mount_option_present(&mount_options, "daemon") || mountpoint_arg.len() >= 2;
    let mountpoint_arg = mountpoint_arg.last().unwrap(); // intentionally, because first argument is "dummy" when fstab is used.

    let connection_string = matches.get_one::<String>("conn");
    let connection_string = mount_option_string(&mount_options, "conn", connection_string.cloned()).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });

    let use_raw = !matches.get_flag("no-raw") && !mount_option_present(&mount_options, "no-raw");
    let magic = !matches.get_flag("no-magic") && !mount_option_present(&mount_options, "no-magic");
    let mirror = matches.get_one::<String>("mirror").map(|s| s.as_str()).unwrap_or("0");
    let mirror: u8 = mirror.parse().unwrap_or(0);
    let mirror = mount_option_int(&mount_options, "mirror", mirror).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    let threads = matches.get_one::<String>("threads").unwrap();
    let threads: usize = threads.parse().unwrap_or(8);
    let threads: usize = mount_option_int(&mount_options, "threads", threads).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });

    let log_file = matches.get_one::<String>("log-file");
    let log_file = mount_option_string(&mount_options, "log-file", log_file.cloned()).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });

    if mount_option_present(&mount_options, "rw") {
        eprintln!("asmfs is read-only; mount option 'rw' is not supported");
        std::process::exit(2);
    }

    if mirror > 2 {
        eprintln!("mirror must be 0, 1, or 2");
        std::process::exit(2);
    }

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

    let mut options = vec![MountOption::FSName("asmfs".to_string())];
    if matches.get_flag("auto-unmount") || mount_option_present(&mount_options, "auto-unmount") || mount_option_present(&mount_options, "auto_unmount") {
        options.push(MountOption::AutoUnmount);
    }

    let allow_root = mount_option_present(&mount_options, "allow_root");
    let allow_other = mount_option_present(&mount_options, "allow_other");

    let acl = match (allow_root, allow_other) {
        (false, false) => SessionACL::Owner,
        (true, false) => SessionACL::RootAndOwner,
        (false, true) => SessionACL::All,
        (true, true) => {
            eprintln!("mount options 'allow_root' and 'allow_other' are mutually exclusive");
            std::process::exit(2);
        }
    };

    if matches.get_flag("fake") {
        return;
    }

    options.push(MountOption::CUSTOM("max_read=33554432".into())); // 32MB max read
    options.push(MountOption::RO); // force read-only
    options.push(MountOption::Async);

    let mut cfg = Config::default();
    cfg.acl = acl;
    cfg.n_threads = Some(threads);
    cfg.clone_fd = true;
    cfg.mount_options = options;

    let mut status_pipe = start_daemon(daemon);

    let asmfs = match AsmFS::new(mountpoint_string, connection_string, use_raw, magic, mirror) {
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

    if status_pipe.is_some() {
        let log_path = log_file.as_deref().map(Path::new);

        if let Err(e) = redirect_daemon_stdio(log_path) {
            startup_failed(&mut status_pipe, &format!("Failed to redirect daemon standard streams: {e}"));
        }

        let mut pipe = status_pipe.take().unwrap();

        if pipe.write_all(b"OK\n").and_then(|_| pipe.flush()).is_err() {
            // The parent may have disappeared, so there is nowhere useful left to report this error.
            std::process::exit(1);
        }

        drop(pipe);
    }

    if let Err(e) = background.join() {
        eprintln!("FUSE session failed: {e}");
        std::process::exit(1);
    }

}

fn mount_option_present(
    options: &[(&str, Option<&str>)],
    name: &str,
) -> bool {
    options.iter().any(|(key, _)| *key == name)
}

fn mount_option_int<T>(
    options: &[(&str, Option<&str>)],
    name: &str,
    fallback: T,
) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match options.iter().rev().find(|(key, _)| *key == name) {
        None => Ok(fallback),
        Some((_, None)) => {
            Err(format!("mount option '{name}' requires a value"))
        }
        Some((_, Some(value))) => value
            .parse::<T>()
            .map_err(|e| format!("invalid value for mount option '{name}': {e}")),
    }
}

fn mount_option_string(
    options: &[(&str, Option<&str>)],
    name: &str,
    fallback: Option<String>,
) -> Result<Option<String>, String> {
    match options.iter().rev().find(|(key, _)| *key == name) {
        None => Ok(fallback),
        Some((_, None)) => {
            Err(format!("mount option '{name}' requires a value"))
        }
        Some((_, Some(value))) => Ok(Some((*value).to_owned())),
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

fn redirect_daemon_stdio(log_file: Option<&Path>) -> std::io::Result<()> {
    let devnull = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?;

    if unsafe { libc::dup2(devnull.as_raw_fd(), 0) } == -1 {
        return Err(std::io::Error::last_os_error());
    }

    let log = match log_file {
        Some(path) => Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?,
        ),
        None => None,
    };

    let output_fd = match &log {
        Some(file) => file.as_raw_fd(),
        None => devnull.as_raw_fd(),
    };

    for target_fd in [1, 2] {
        if unsafe { libc::dup2(output_fd, target_fd) } == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }

    Ok(())
}
