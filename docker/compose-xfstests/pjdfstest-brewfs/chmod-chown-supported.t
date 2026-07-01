#!/bin/sh
# vim: filetype=sh noexpandtab ts=8 sw=8

desc="brewfs: chmod and chown cover supported file kinds"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..45"

n0=`namegen`
n1=`namegen`
n2=`namegen`
n3=`namegen`

expect 0 mkdir ${n3} 0755
cdir=`pwd`
cd ${n3}

expect 0 create ${n0} 0755
expect 0 chmod ${n0} 0111
expect 0111 stat ${n0} mode
expect 0 symlink ${n0} ${n1}
mode=`${fstest} lstat ${n1} mode`
expect 0 chmod ${n1} 0222
expect 0222 stat ${n1} mode
expect 0222 stat ${n0} mode
expect ${mode} lstat ${n1} mode
expect 0 unlink ${n1}
expect 0 unlink ${n0}

expect 0 mkdir ${n0} 0755
expect 0 chmod ${n0} 0111
expect 0111 stat ${n0} mode
expect 0 rmdir ${n0}

expect 0 create ${n0} 0644
ctime1=`${fstest} stat ${n0} ctime`
sleep 1
expect 0 chmod ${n0} 0111
ctime2=`${fstest} stat ${n0} ctime`
test_check $ctime1 -lt $ctime2
expect 0 unlink ${n0}

expect 0 create ${n0} 0644
ctime1=`${fstest} stat ${n0} ctime`
sleep 1
expect EPERM -u 65534 chmod ${n0} 0111
ctime2=`${fstest} stat ${n0} ctime`
test_check $ctime1 -eq $ctime2
expect 0 unlink ${n0}

expect 0 create ${n0} 0644
expect 0 chown ${n0} 123 456
expect 123,456 lstat ${n0} uid,gid
expect 0 chown ${n0} 0 0
expect 0,0 lstat ${n0} uid,gid

expect 0 symlink ${n0} ${n1}
uidgid=`${fstest} lstat ${n1} uid,gid`
expect 0 chown ${n1} 123 456
expect 123,456 stat ${n1} uid,gid
expect 123,456 stat ${n0} uid,gid
expect ${uidgid} lstat ${n1} uid,gid
expect 0 unlink ${n1}
expect 0 unlink ${n0}

expect 0 mkdir ${n0} 0755
expect 0 chown ${n0} 123 456
expect 123,456 lstat ${n0} uid,gid
expect 0 rmdir ${n0}

expect 0 create ${n0} 0644
expect 0 chown ${n0} 65534 65534
expect EPERM -u 65534 -g 65534 chown ${n0} 65533 65533
expect EPERM -u 65533 -g 65533 chown ${n0} 65534 65534
expect 0 unlink ${n0}

cd ${cdir}
expect 0 rmdir ${n3}
