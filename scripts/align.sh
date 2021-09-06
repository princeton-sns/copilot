#!/bin/bash

fn=$1
outfn=$2
line=$(cat $fn | head -n 1)
nanosecs=$(echo $line | awk -F" " '{print $1}' | awk -F"." '{print $2}')
secs=$(echo $line | awk -F" " '{print $1}' | awk -F"." '{print $1}' | awk -F":" '{print $3}')
mins=$(echo $line | awk -F" " '{print $1}' | awk -F":" '{print $2}')
hrs=$(echo $line | awk -F" " '{print $1}' | awk -F":" '{print $1}')
while read -r line; do 
	IFS=':.	' read -r -a arr <<< "$line"
	newt=$(echo "((${arr[0]}-$hrs)*3600+(${arr[1]}-$mins)*60+${arr[2]}-$secs)*1000000000+${arr[3]}-$nanosecs" | bc -l)
	echo -e "${newt}\t${arr[4]}" >> $outfn 
done < $fn


