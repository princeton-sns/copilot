#!/bin/bash
indir=$1
outdir=$2
n=$3 #number of clients
st=$4
et=$5
for i in $(seq 0 $((n-1))); do
	./cut.sh "${indir}/client-${i}.timestamps.orig.txt" $st $et "${outdir}/client${i}.txt"
done;

for i in $(seq 0 $((n-1))); do
	./align.sh "${outdir}/client$i.txt" "${outdir}/c$i.txt" &
done;
