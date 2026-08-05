Name: asmfs
Version: %{asmfs_version}
Release: 1%{?dist}
Summary: A read-only FUSE filesystem that exposes Oracle ASM files
License: MIT
AutoReqProv: no
Requires: libaio, fuse3
%description
n/a

%install
install -Dm755 /opt/asmfs-src/target/%{asmfs_build_type}/asmfs %{buildroot}/opt/asmfs/asmfs
install -d %{buildroot}%{_bindir}
ln -s /opt/asmfs/asmfs %{buildroot}%{_bindir}/fuse3.asmfs

%files
%dir /opt/asmfs/
/opt/asmfs/asmfs
%{_bindir}/fuse3.asmfs
