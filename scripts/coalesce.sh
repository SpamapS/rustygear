#!/bin/bash
set -eux

cleanup () {
    if [[ -n "${outfile:-}" ]] ; then
        rm -f $outfile
    fi
    if [[ -n "${outfile2:-}" ]] ; then
        rm -f $outfile2
    fi
    if [[ -n "${ref:-}" ]] ; then
        rm -f $ref
    fi
}
trap cleanup EXIT
outfile=$(mktemp -t gearman.XXXXXXXX)
outfile2=$(mktemp -t gearman.zz.XXXXXXXX)

gearman -u myunique -P -f foo > $outfile & <<EOF
a
c
b
EOF

gearman -u myunique -P -f foo > $outfile & <<EOF
z
z
1
EOF

# let them submit
sleep 1

gearman -w -f foo -c 1 sort
wait
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
elif ! cmp $ref $outfile2 ; then
    diff $ref $outfile2
    echo FAIL! outfile2
    exit 1
else
    cleanup
    trap - EXIT
    echo OK!
fi
