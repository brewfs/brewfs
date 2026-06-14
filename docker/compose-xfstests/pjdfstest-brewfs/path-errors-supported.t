#!/bin/sh
# vim: filetype=sh noexpandtab ts=8 sw=8

desc="brewfs: path and existence errors for supported file kinds"

dir=`dirname $0`
. ${dir}/../misc.sh

require link

echo "1..58"

n0=`namegen`
n1=`namegen`
n2=`namegen`

expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/${n1} 0644
expect ENOTDIR chmod ${n0}/${n1}/test 0644
expect ENOTDIR chown ${n0}/${n1}/test 65534 65534
expect ENOTDIR lchown ${n0}/${n1}/test 65534 65534
expect ENOTDIR link ${n0}/${n1}/test ${n0}/${n2}
expect ENOTDIR mkdir ${n0}/${n1}/test 0755
expect ENOTDIR open ${n0}/${n1}/test O_RDONLY
expect ENOTDIR open ${n0}/${n1}/test O_CREAT 0644
expect ENOTDIR rename ${n0}/${n1}/test ${n0}/${n2}
expect ENOTDIR rmdir ${n0}/${n1}/test
expect 0 unlink ${n0}/${n1}
expect 0 rmdir ${n0}

expect 0 create ${n0} 0644
expect ENOTDIR rmdir ${n0}
expect 0 unlink ${n0}

expect 0 symlink ${n1} ${n0}
expect ENOTDIR rmdir ${n0}
expect 0 unlink ${n0}

expect 0 create ${n0} 0644
expect EEXIST mkdir ${n0} 0755
expect EEXIST symlink test ${n0}
expect EEXIST open ${n0} O_CREAT,O_EXCL 0644
expect 0 unlink ${n0}

expect 0 mkdir ${n0} 0755
expect EEXIST mkdir ${n0} 0755
expect EEXIST symlink test ${n0}
expect EEXIST open ${n0} O_CREAT,O_EXCL 0644
expect 0 rmdir ${n0}

expect 0 symlink target ${n0}
expect EEXIST mkdir ${n0} 0755
expect EEXIST symlink test ${n0}
expect EEXIST open ${n0} O_CREAT,O_EXCL 0644
expect 0 unlink ${n0}

expect 0 create ${n0} 0644
expect 0 create ${n1} 0644
expect EEXIST link ${n0} ${n1}
expect 0 unlink ${n1}
expect 0 unlink ${n0}

expect 0 mkdir ${n0} 0755
expect 0 create ${n1} 0644
expect ENOTDIR rename ${n0} ${n1}
expect dir lstat ${n0} type
expect regular lstat ${n1} type
expect 0 unlink ${n1}
expect 0 create ${n1} 0644
expect EISDIR rename ${n1} ${n0}
expect dir lstat ${n0} type
expect regular lstat ${n1} type
expect 0 unlink ${n1}
expect 0 rmdir ${n0}

expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 create ${n1}/${n2} 0644
expect "EEXIST|ENOTEMPTY" rename ${n0} ${n1}
expect 0 unlink ${n1}/${n2}
expect 0 rmdir ${n1}
expect 0 rmdir ${n0}
