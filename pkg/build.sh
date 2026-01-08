#!/bin/bash -eu
#
#

CFG_DIST_LIST=(
    oraclelinux:10
    oraclelinux:9
    oraclelinux:8
)

CFG_RELEASE="${1:-debug}"

CFG_SRC_PATH="$(pwd)"
CFG_SRC_PATH="${CFG_SRC_PATH%/*}"
CFG_VERSION="$(cat ../Cargo.toml  | grep version | cut -d'=' -f2 | sed 's/"//g' | xargs)"

for l_dist in "${CFG_DIST_LIST[@]}"
do
    echo "Processing: $l_dist"

    l_flag=''
    l_build_type='debug'
    if [ "$CFG_RELEASE" == 'release' ]
    then
        l_flag='--release'
        l_build_type='release'
    fi
    
    docker run --rm \
        -v "$CFG_SRC_PATH:/opt/asmfs-src" \
        -w "/opt/asmfs-src" \
        $l_dist \
        bash -c "
            dnf --refresh update && dnf install -y gcc make curl fuse3-devel rpm-build &&
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y &&
            source ~/.cargo/env &&
            cargo clean && 
            cargo build $l_flag &&
            rpmbuild -bb --define 'asmfs_build_type ${l_build_type}' --define 'asmfs_version ${CFG_VERSION}' /opt/asmfs-src/pkg/asmfs.spec &&
            cp -v /root/rpmbuild/RPMS/x86_64/*.rpm /opt/asmfs-src/pkg/ &&
            dnf install /root/rpmbuild/RPMS/x86_64/*.rpm
"
done

