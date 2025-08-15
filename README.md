# ASMFS

A read-only FUSE filesystem that exposes **raw** Oracle ASM files.

![Experimental](https://img.shields.io/badge/status-experimental-orange)

:warning: This project is experimental, not well-tested, and still under development.

## Compile

```
$ cargo build
```

## Help

```
$ ./asmfs -help
Usage: asmfs [OPTIONS] <MOUNT_POINT>

Arguments:
  <MOUNT_POINT>  Act as a client, and mount FUSE at given path

Options:
      --conn <CONNECTION_STRING>  Connection string to remote ASM instance - user/pass@host:port/service (user must have sysdba)
      --auto-unmount              Automatically unmount on process exit
      --allow-root                Allow root user to access filesystem
  -h, --help                      Print help
  -V, --version                   Print version
```

## Local Example

```
$ . oraenv
ORACLE_SID = [+ASM] ? +ASM

$ ./asmfs /mnt/asmfs/
```

## Remote Example

```
$ ./asmfs --conn user/pass@hostname:1521/+ASM /mnt/asmfs/
```

## Warning!

This is __not__ meant for production usage. First of all, it uses _undocumented_ `dbms_diskgroup.read()` call to
access raw blocks of files. Which, it seems, according to my testing, returns __raw__ data as written in ASM diskgroup. Meaning, if you
copy files from this filesystem, be aware that first few bytes will likely be different to what you would get if you'd use `rman backup/restore` or `asmcmd cp`
to copy files to local filesystem.

This project is **experimental**, not well-tested, and still under development.

**Contributions are welcome!** Ideas, issues, and pull requests are appreciated.

## Screenshot (demo)

![asmfs demo](https://github.com/usrecnik/asmfs/blob/main/doc/asmfs_screenshot.png?raw=true)


