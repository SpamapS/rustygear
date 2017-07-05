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

gearman -w -c 4006 -f foo2 -f foo sort &
sleep 1
echo make lots of jobs
set +x
for i in $(seq 0 1000) ; do echo -e "a\njob\nfor\n$i\n" >> $outdir/500z.txt ; done
set -x
echo And now make a client pushing jobs into it
timeout 30s gearman -b -P -f foo2 -n < $outdir/500z.txt
wait

cleanup
trap - EXIT
echo OK!
