#!/bin/sh
# vim: filetype=sh noexpandtab ts=8 sw=8

desc="brewfs: rename over multiply-linked regular destination updates old inode"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..10"

src=`namegen`
dst=`namegen`
dstlnk=`namegen`
parent=`namegen`

expect 0 mkdir ${parent} 0755
cdir=`pwd`
cd ${parent}

expect 0 create ${src} 0644
expect 0 create ${dst} 0644
expect 0 link ${dst} ${dstlnk}
ctime1=`${fstest} lstat ${dstlnk} ctime`
sleep 1

expect 0 rename ${src} ${dst}

expect regular,1 lstat ${dstlnk} type,nlink
ctime2=`${fstest} lstat ${dstlnk} ctime`
test_check $ctime1 -lt $ctime2

expect 0 unlink ${dst}
expect 0 unlink ${dstlnk}

cd ${cdir}
expect 0 rmdir ${parent}
