#! /bin/sh
# vim: filetype=sh noexpandtab ts=8 sw=8

desc="brewfs: utimensat changes timestamps on supported file kinds"

dir=`dirname $0`
. ${dir}/../misc.sh

require "utimensat"

echo "1..12"

n0=`namegen`
n1=`namegen`

expect 0 mkdir ${n1} 0755
cdir=`pwd`
cd ${n1}

DATE1=1900000000
DATE2=1950000000

expect 0 create ${n0} 0644
expect 0 open . O_RDONLY : utimensat 0 ${n0} $DATE1 0 $DATE2 0 0
expect $DATE1 lstat ${n0} atime
expect $DATE2 lstat ${n0} mtime
expect 0 unlink ${n0}

expect 0 mkdir ${n0} 0755
expect 0 open . O_RDONLY : utimensat 0 ${n0} $DATE1 0 $DATE2 0 0
expect $DATE1 lstat ${n0} atime
expect $DATE2 lstat ${n0} mtime
expect 0 rmdir ${n0}

cd ${cdir}
expect 0 rmdir ${n1}
