use std::io;
use std::sync::OnceLock;
use std::collections::HashMap;
use std::process::Command;
use log::info;

static AFD_MAP: OnceLock<HashMap<String, String>> = OnceLock::new();

pub fn get_afd_map() -> &'static HashMap<String, String> {
    AFD_MAP.get_or_init(|| get_afd_disk_mapping().expect("failed to get afd disk mapping"))
}

/*
 * Example output of `afdtool -getdevlist`:
 *
 * --------------------------------------------------------------------------------
 * Label                     Path
 * ================================================================================
 * DATA1                     /dev/sdd
 * DATA2                     /dev/sdb
 * DATA3                     /dev/sde
 *
 */
fn get_afd_disk_mapping() -> io::Result<HashMap<String, String>> {

    info!("Running 'afdtool -getdevlist'....");
    let mut map: HashMap<String, String> = HashMap::new();

    let output = Command::new("afdtool")
        .args(["-getdevlist"])
        .output()?;

    if !output.status.success() {
        return Err(io::Error::new(io::ErrorKind::Other, "asmcmd failed"));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Skip header lines
    for line in stdout.lines().skip(3) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            map.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
        }
    }

    info!("AFD device map: \n---\n{:?}\n---\n", map);

    Ok(map)
}