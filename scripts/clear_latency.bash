#!/bin/bash 

username="$USER"
dirpath="/proj/cops/${username}/K2/"
dcl_config_full=$1
all_servers=($(cat $dcl_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs))
all_servers=$(echo "echo ${all_servers[@]}" | bash)
num_dcs=$(cat $dcl_config_full | grep num_dcs | awk -F"=" '{ print $2 }')
num_servers=$(echo $all_servers | wc -w)
num_servers_per_dc=$((num_servers / num_dcs))
ips=() # ib0 ips
server_ips=()
#
# find ips of servers and ib0 interface
while read line; do
    if [[ $line == *"num_dcs"* ]]
    then
	    continue;
    fi

    name=$(echo $line | awk -F"=" '{print $2}')
    echo $name
    #ip=$(host $name | grep "has address" | awk -F" " '{print $NF}')
    #ip=$(getent hosts $name | awk -F" " '{print $1}')
    #ips+=($ip)

    #name=$(echo $name | awk -F"-" '{print $1}')
    server_ip=$(getent hosts $name | awk -F" " '{print $1}')
    server_ips+=($server_ip)
    echo $server_ip $ip
done < $dcl_config_full 

# clean up ib interface
for ip in ${server_ips[@]}; do
        ssh $ip "cd ${dirpath}; sudo tc qdisc del dev \$(./getifname.bash) root;"
done;
