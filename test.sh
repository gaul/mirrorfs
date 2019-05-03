#!/bin/sh

set -o errexit
set -o nounset

# setup
fusermount -q -u mnt || true
rm -rf a b
mkdir a b
./mirrorfs a b mnt

# test read and write
echo foo > mnt/foo
cat mnt/foo > /dev/null
[ -f mnt/foo ]

# test rename
mv mnt/foo mnt/bar
[ ! -f mnt/foo ]
[ -f mnt/bar ]

# test metadata
[ ! -x mnt/bar ]
chmod +x mnt/bar
[ -x mnt/bar ]

# clean up
fusermount -u -z mnt
rm -rf a b
