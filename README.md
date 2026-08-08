# ASMFS

A read-only FUSE filesystem that exposes Oracle ASM files.

![Experimental](https://img.shields.io/badge/status-experimental-orange)

:warning: This project is experimental, not well-tested, and still under development.

## Compile

```
$ cargo build
```

## Help

```
Usage: asmfs [OPTIONS] <MOUNT_POINT>

Arguments:
  <MOUNT_POINT>  Act as a client, and mount FUSE at given path

Options:
      --conn <CONNECTION_STRING>  Connection string to remote ASM instance - user/pass@host:port/service (user must have sysdba)
      --no-raw                    Use DBMS_DISKGROUP.READ() instead of raw device access
      --mirror <mirror>           0=>primary copy, 1=>first redundant copy, 2=>second redundant copy [default: 0]
      --auto-unmount              Automatically unmount on process exit
      --uid <UID>                 UID reported as the owner of all filesystem entries [default: effective UID of asmfs]
      --gid <GID>                 GID reported as the owner of all filesystem entries [default: effective GID of asmfs]
      --daemon                    Mount in the background
      --allow-root                Allow root user to access filesystem
  -h, --help                      Print help
  -V, --version                   Print version
```

Options may also be supplied in the comma-separated `-o` form, for example:

```
-o allow_other,uid=1000,gid=200,threads=8
```

## Two Modes

There are two different implementations on how `asmfs` can read files from ASM:

* A raw access to block devices, which is the default and works as described here (link todo).
* `dbms_diskgroup.read()` which is used only if you explicitly specify `--no-raw`. The limitations of this approach are described in [this blog post](https://blog.srecnik.info/asmfs-and-dbmsdiskgroupread)

## Installation

Grab one of the `.rpm` files from [asmfs releases](https://github.com/usrecnik/asmfs/releases) and simply run:

```
dnf install ./asmfs-VERSION.x86_64.rpm
```

## Examples

### Raw mode with udev

Run under `grid` user:

```
/opt/asmfs/asmfs /mnt/asmfs/
```

### Raw mode with AFD

Run under `root` user (because otherwise AFD won't allow non-oracle I/O to ASM disks):

```
/opt/asmfs/asmfs --conn username/password@db-server:1521/+ASM /mnt/asmfs/
```

(such `username` can be created on `+ASM` instance with `CREATE USER` syntax. Such user must also be granted at lease `SYSDBA` privilege.)

### `dbms_diskgroup.read` locally

```
$ . oraenv
ORACLE_SID = [+ASM] ? +ASM

$ /opt/asmfs/asmfs/asmfs --no-raw /mnt/asmfs/
```

### `dbms_diskgroup.read` remotely

```
$ /opt/asmfs/asmfs/asmfs --no-raw --conn user/pass@hostname:1521/+ASM /mnt/asmfs/
```
### Umounting

```
fusermount3 -u /mnt/asmfs/
```

## Debugging

If you run into issues, you can find out more about what's going on by setting those env vars:

```
RUST_BACKTRACE=1 RUST_LOG=info /opt/asmfs/asmfs ...
```

## Warning!

This is __not__ meant for production usage. 

This project is **experimental**, not well-tested, and still under development.

**Contributions are welcome!** Ideas, issues, and pull requests are appreciated.

## Intended Usage

ASMFS is intended for **quiescent** ASM files. Those are files that are not being modified or actively used by a database.
ASMFS can read live database files without modifying or corrupting them because it is read-only, but the resulting data
may be inconsistent because different blocks can be read at different points in time.

ASMFS derives inode numbers from `REFERENCE_INDEX` and `ALIAS_INDEX`; `GROUP_NUMBER` is embedded in `REFERENCE_INDEX`.
ASM incarnation fields are _not_ part of this identity. ASMFS assumes that group numbers and the ASM namespace remain
stable while mounted (newly created files do appear while mounted, including new archived logs). If a disk-group remount 
or ASM restart changes group numbers, ASMFS must be remounted; NFS clients must also unmount and remount the export in this case.

## Screenshot (demo)

![asmfs demo](https://github.com/usrecnik/asmfs/blob/main/doc/asmfs_screenshot.png?raw=true)
