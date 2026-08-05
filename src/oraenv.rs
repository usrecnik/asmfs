use std::env;
use std::ffi::{CString, OsStr, OsString};
use std::fs;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;

const BOOTSTRAP_GUARD: &str = "ASMFS_ENV_BOOTSTRAP";
const PMON_PREFIX: &[u8] = b"asm_pmon_+ASM";


pub(crate) fn bootstrap_oracle_env(args: &[OsString]) {

    // do nothing if ASMFS_ENV_BOOTSTRAP is already set
    if env::var_os(BOOTSTRAP_GUARD).is_some() {
        return;
    }

    let existing_oracle_home = env::var_os("ORACLE_HOME");
    let existing_oracle_sid = env::var_os("ORACLE_SID");
    let existing_library_path = env::var_os("LD_LIBRARY_PATH");

    if existing_oracle_home.is_some()
        && existing_oracle_sid.is_some()
        && existing_library_path.is_some()
    {
        // Everything was supplied manually. Do not inspect or validate it, just use it.
        println!("Oracle environment already set (ORACLE_SID, ORACLE_HOME, LD_LIBRARY_PATH). Skipping bootstrap.");
        return;
    }

    let discovered_instance = find_asm_instance();
    let Some((pid, sid)) = discovered_instance.as_ref() else {
        eprintln!("No ASM instance found. Skipping bootstrap.");
        return;
    };

    let Some(home) = oracle_home_from_executable(*pid) else {
        eprintln!("No Oracle home found for ASM instance. Skipping bootstrap.");
        return;
    };

    let lib_dir = home.join("lib");
    let new_library_path = match existing_library_path.as_ref() {
        Some(existing) if !existing.is_empty() => {
            let mut value = lib_dir.as_os_str().to_os_string();
            value.push(":");
            value.push(existing);
            value
        }
        _ => lib_dir.as_os_str().to_os_string(),
    };

    // SAFETY: this function is called before logging, clap, FUSE, or any
    // other code that could create another thread. This is why this oraenv.rs intentionally
    // avoids using info!() or warn!() calls.
    unsafe {
        env::set_var("ORACLE_HOME", &home);
        env::set_var("ORACLE_SID", sid);
        env::set_var("LD_LIBRARY_PATH", new_library_path.clone());
        env::set_var(BOOTSTRAP_GUARD, "1");
    }

    println!("Re-executing with:");
    println!("  ORACLE_SID={}", sid.to_string_lossy());
    println!("  ORACLE_HOME={}", home.display());
    println!("  LD_LIBRARY_PATH={}", new_library_path.display());

    reexec(args);
}

/**
 * Returns ORACLE_SID of the ASM instance and its PID.
 */
fn find_asm_instance() -> Option<(u32, OsString)> {
    let proc_entries = fs::read_dir("/proc").ok()?;
    let mut matches = Vec::new();

    for entry in proc_entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };

        let Ok(cmdline) = fs::read(entry.path().join("cmdline")) else {
            continue;
        };

        // argv[0] is everything before the first NUL byte.
        let command = cmdline
            .split(|byte| *byte == 0)
            .next()
            .unwrap_or_default();

        // Ignore non-ASM processes, including asm_pmon_+APX.
        if !command.starts_with(PMON_PREFIX) {
            continue;
        }

        // Extract the complete SID after the final underscore:
        // asm_pmon_+ASM2 -> +ASM2
        let Some(last_underscore) =
            command.iter().rposition(|byte| *byte == b'_')
        else {
            continue;
        };

        let sid = &command[last_underscore + 1..];

        if !sid.is_empty() {
            matches.push((pid, OsString::from_vec(sid.to_vec())));
        }
    }

    // If several ASM instances are found, use the one with the lowest PID.
    matches.sort_by_key(|(pid, _)| *pid);

    if matches.len() > 1 {
        eprintln!(
            "asmfs: warning: multiple ASM instances found; using pid {}",
            matches[0].0
        );
    }

    matches.into_iter().next()
}

fn oracle_home_from_executable(pid: u32) -> Option<PathBuf> {
    let executable = fs::read_link(format!("/proc/{pid}/exe")).ok()?;

    if executable.file_name()? != OsStr::new("oracle") {
        return None;
    }

    let bin_dir = executable.parent()?;

    if bin_dir.file_name()? != OsStr::new("bin") {
        return None;
    }

    Some(bin_dir.parent()?.to_path_buf())
}

fn reexec(args: &[OsString]) -> ! {
    let args: Vec<CString> = args
        .iter()
        .map(|arg| CString::new(arg.as_bytes()).expect("argv contains NUL"))
        .collect();

    let mut argv: Vec<*const libc::c_char> =
        args.iter().map(|arg| arg.as_ptr()).collect();

    argv.push(std::ptr::null());

    // SAFETY: argv is NUL-terminated, and every pointer refers to a CString
    // retained for the duration of the call.
    unsafe {
        libc::execv(c"/proc/self/exe".as_ptr(), argv.as_ptr());
    }

    eprintln!(
        "asmfs: re-exec failed: {}",
        std::io::Error::last_os_error()
    );
    std::process::exit(1);
}