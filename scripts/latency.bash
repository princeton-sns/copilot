rm  alllatencies.txt 2> /dev/null
rm  sorted.txt 2> /dev/null
rm percentilesnew.txt 2> /dev/null
for i in $(seq 0 $((clients-1))); do
	cat client-$i.latency.all.txt >> alllatencies.txt;
done

sort -n alllatencies.txt > sorted.txt

#lines=$(cat alllatencies.txt)
#echo $lines

numlines=$(cat alllatencies.txt | wc -l)

#echo $numlines
#echo "" > percentilesnew.txt


#for i in 10 250 500 750 900 950 990 999; do
for i in $(seq 1 99); do
	j=$((numlines * i / 100))
	#echo $j
	val=$(sed "${j}q;d" sorted.txt)
	echo -e "$i\t$val" >> percentilesnew.txt
done
j=$((numlines * 999 / 1000))
val=$(sed "${j}q;d" sorted.txt)
echo -e "99.9\t$val" >> percentilesnew.txt

j=$((numlines-1))
val=$(sed "${j}q;d" sorted.txt)
echo -e "100\t$val" >> percentilesnew.txt

# clean up
rm alllatencies.txt
rm sorted.txt 


#cd ../
