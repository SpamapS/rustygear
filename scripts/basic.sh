#!/bin/bash
set -eux

cleanup () {
    if [[ -n "${outfile:-}" ]] ; then
        rm -f $outfile
    fi
    if [[ -n "${ref:-}" ]] ; then
        rm -f $ref
    fi
}
trap cleanup EXIT
outfile=$(mktemp -t gearman.XXXXXXXX)

strace -f gearman -c 1 -w -f foo sort &
gearman -P -f foo > $outfile <<EOF
a
c
b
EOF
wait

ref=$(mktemp -t gearman.ref.XXXXXXXX)
cat > $ref <<EOF
foo: a
b
c
EOF
if ! cmp $ref $outfile ; then
    diff $ref $outfile
    echo FAIL!
    exit 1
else
    cleanup
    trap - EXIT
    echo OK!
fi
