# OpenASMFS

A read-only FUSE filesystem that exposes **raw** Oracle ASM files using internal APIs.

## Compile

```
$ cargo build
```

## Usage

```
$ . oraenv
ORACLE_SID = [+ASM] ? +ASM

$ RUST_LOG=info ./asmfs /opt/asmfs_mnt/
```

## Warning!

This is __not__ meant for production usage. First of all, it uses _undocumented_ `dbms_diskgroup.read()` call to
access raw blocks of files. Which, it seems, according to my testing, returns __raw__ data as written in ASM diskgroup. Meaning, if you
copy files from this filesystem, be aware that first few bytes will likely be different to what you would get if you'd use `rman backup/restore` or `asmcmd cp`
to copy files to local filesystem.

This project is **experimental**, not well-tested, and still under development.

**Contributions are welcome!** Ideas, issues, and pull requests are appreciated.

