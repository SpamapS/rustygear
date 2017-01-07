#!/bin/bash
set -eux

cleanup () {
    if [[ -n "${outdir:-}" ]] ; then
        rm -rf $outdir
    fi
    if [[ -n "${ref:-}" ]] ; then
        rm -f $ref
    fi
}
trap cleanup EXIT
outdir=$(mktemp -d -t gearman.rr.XXXXXXXX)
outfile=$outdir/sorted.txt

gearman -w -c 5003 -f foo2 -f foo sort &
sleep 1
echo make lots of jobs
for i in $(seq 0 1000) ; do echo -e "a\njob\nfor\n$i\n" >> $outdir/500z.txt ; done
echo And now make a client pushing jobs into it
gearman -b -P -f foo2 -n < $outdir/500z.txt &
# and now the regular foo client
timeout 10s gearman -P -f foo > $outfile <<EOF
a
c
b
EOF
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
else
    cleanup
    trap - EXIT
    echo OK!
fi
