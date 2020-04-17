#!/usr/bin/env bash
set -eux

cleanup () {
    if [[ -n "${outdir:-}" ]] ; then
        rm -rf $outdir
    fi
}
trap cleanup EXIT
outdir=$(mktemp -d gearman.XXXXXXXX)
export TMPDIR=$outdir
outfile=$(mktemp -t gearman.ab.XXXXXXXX)
outfile2=$(mktemp -t gearman.zz.XXXXXXXX)
infile=$(mktemp -t gearman.ab.in.XXXXXXXX)
infile2=$(mktemp -t gearman.zz.in.XXXXXXXX)

cat > $infile <<EOF
a
c
b
EOF
cat > $infile2 <<EOF
z
z
1
EOF
timeout 11s gearman -u myunique -P -f foo < $infile > $outfile &
sleep 1
timeout 10s gearman -u myunique -P -f foo < $infile2 > $outfile2 &

# let them submit
sleep 1

timeout 5s gearman -w -f foo -c 1 sort
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
