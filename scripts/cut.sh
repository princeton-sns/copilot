#!/bin/bash
fn=$1
st=$2
et=$3
ofn=$4

sl=$(grep -n $st $fn | head -n 1 | awk -F":" '{print $1}')
el=$(grep -n $et $fn | head -n 1 | awk -F":" '{print $1}')
el=$((el-1))
n=$((el-sl+1))
#echo $sl
#echo $el
#echo $n
cat $fn | head -n $el | tail -n $n > ${ofn}

