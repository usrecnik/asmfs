Name: asmfs
Version: %{asmfs_version}
Release: 1%{?dist}
Summary: A read-only FUSE filesystem that exposes Oracle ASM files
License: MIT
AutoReqProv: no
BuildRequires: systemd-rpm-macros
%{?systemd_requires}
Requires: libaio, fuse3
%description
n/a

%install
install -Dm0755 /opt/asmfs-src/target/%{asmfs_build_type}/asmfs %{buildroot}/opt/asmfs/asmfs
install -Dm0755 /opt/asmfs-src/pkg/asmfs-wait-for-asm.sh %{buildroot}/opt/asmfs/asmfs-wait-for-asm.sh
install -d %{buildroot}%{_bindir}
ln -s /opt/asmfs/asmfs %{buildroot}%{_bindir}/fuse3.asmfs
install -Dm0644 /opt/asmfs-src/pkg/asmfs-wait-for-asm.service %{buildroot}%{_unitdir}/asmfs-wait-for-asm.service

%post
%systemd_post asmfs-wait-for-asm.service

%preun
%systemd_preun asmfs-wait-for-asm.service

%postun
%systemd_postun asmfs-wait-for-asm.service

%files
%dir /opt/asmfs/
/opt/asmfs/asmfs
/opt/asmfs/asmfs-wait-for-asm.sh
%{_bindir}/fuse3.asmfs
%{_unitdir}/asmfs-wait-for-asm.service
