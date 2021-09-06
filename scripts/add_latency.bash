#!/bin/bash

username="$USER"
dcl_config_full=$1
all_servers=($(cat $dcl_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs))
all_servers=$(echo "echo ${all_servers[@]}" | bash)
num_dcs=$(cat $dcl_config_full | grep num_dcs | awk -F"=" '{ print $2 }')
num_servers=$(echo $all_servers | wc -w)
num_servers_per_dc=$((num_servers / num_dcs))
ips=()        # ib0 ips
server_ips=() # server ip to ssh

getifname() {
  echo $(ifconfig | grep -B1 "10.1.1" | head -n 1 | awk -F" " '{print $1}')
}

# find ips of servers and ib0 interface
while read line; do
  if [[ $line == *"num_dcs"* ]]; then
    continue
  fi

  if [[ $line == *"194.168"* ]]; then
    name=$(echo $line | awk -F"=" '{print $2}')
    echo "2nd ins: "$name
    ips+=($name)
    server_ips+=($name)
    continue
  fi

  name=$(echo $line | awk -F"=" '{print $2}')
  echo $name
  #ip=$(host $name | grep "has address" | awk -F" " '{print $NF}')
  ip=$(getent hosts $name | awk -F" " '{print $1}')
  ips+=($ip)
  server_ips+=($name)

  #name=$(echo $name | awk -F"-" '{print $1}')
  #server_ip=$(getent hosts $name | awk -F" " '{print $1}')
  #server_ips+=($server_ip)
  #echo $server_ip $ip
done <$dcl_config_full

# get latency from command line
latency="100ms"
if [[ "$#" -gt 1 ]]; then
  echo $2
  latency=$2
fi

# clean up and add latency on ib0 interface
for ip in ${server_ips[@]}; do
  ssh $ip "$(typeset -f getifname); sudo tc qdisc del dev \$(getifname) root; sudo tc qdisc add dev \$(getifname) root handle 1: prio; sudo tc qdisc add dev \$(getifname) parent 1:1 handle 2: netem delay $latency;"
done

# add latency only to traffic to filtered servers
#for dci in $(seq 0 $(($num_dcs-1))); do
for dci in 0; do
  for dcj in $(seq 1 $(($num_dcs - 1))); do

    if [[ $dci == $dcj ]]; then
      continue
    fi
    echo "=== Adding latency between DC$dci and DC$dcj ==="
    starti=$num_servers_per_dc*$dci
    startj=$num_servers_per_dc*$dcj

    for i in $(seq 0 $(($num_servers_per_dc - 1))); do

      ipi_ip=${server_ips[starti + i]}
      ipi_ib0=${ips[starti + i]}

      for j in $(seq 0 $(($num_servers_per_dc - 1))); do

        ipj_ip=${server_ips[startj + j]}
        ipj_ib0=${ips[startj + j]}

        echo "DC$dci server $i ($ipi_ip) -- DC$dcj server $j ($ipj_ib0)"
        ssh $ipi_ip "$(typeset -f getifname); sudo tc filter add dev \$(getifname) parent 1:0 protocol ip pref 55 handle ::55 u32 match ip dst $ipj_ib0 flowid 2:1;"

        echo "DC$dcj server $j ($ipj_ip) -- DC$dci server $i ($ipi_ib0)"
        ssh $ipj_ip "$(typeset -f getifname); sudo tc filter add dev \$(getifname) parent 1:0 protocol ip pref 55 handle ::55 u32 match ip dst $ipi_ib0 flowid 2:1;"

      done
    done

  done
done

add_clients_dcs_latency=0
if [[ "$#" -gt 2 ]]; then
  echo $3
  client_dcl_config_full=$3
  add_clients_dcs_latency=1
fi

if [[ $add_clients_dcs_latency -gt 0 ]]; then
  all_clients=($(cat $client_dcl_config_full | grep cassandra_ips | awk -F"=" '{ print $2 }' | xargs))
  all_clients=$(echo "echo ${all_clients[@]}" | bash)
  num_dcs=$(cat $client_dcl_config_full | grep num_dcs | awk -F"=" '{ print $2 }')
  num_clients=$(echo $all_clients | wc -w)
  num_clients_per_dc=$((num_clients / num_dcs))
  if_ips=()     # ib0 ips
  client_ips=() # server ip to ssh

  # find ips of servers and ib0 interface
  while read line; do
    if [[ $line == *"num_dcs"* ]]; then
      continue
    fi

    name=$(echo $line | awk -F"=" '{print $2}')
    echo $name
    #ip=$(host $name | grep "has address" | awk -F" " '{print $NF}')
    ip=$(getent hosts $name | awk -F" " '{print $1}')
    if_ips+=($ip)
    client_ips+=($name)

  done <$client_dcl_config_full

  for ip in ${client_ips[@]}; do
    ssh $ip "sudo tc qdisc del dev \$(getifname) root; sudo tc qdisc add dev \$(getifname) root handle 1: prio; sudo tc qdisc add dev \$(getifname) parent 1:1 handle 2: netem delay $latency;"
  done

  # add latency only to traffic to filtered servers
  # skip dci for client
  for dci in $(seq 0 $(($num_dcs - 1))); do
    for dcj in $(seq 0 $(($num_dcs - 1))); do

      if [[ $dci == $dcj ]]; then
        continue
      fi

      echo "=== Adding latency between clients in DC$dci and DC$dcj ==="
      starti=$num_clients_per_dc*$dci
      startj=$num_servers_per_dc*$dcj

      for i in $(seq 0 $(($num_clients_per_dc - 1))); do
        ipi_ip=${client_ips[starti + i]}
        for j in $(seq 0 $(($num_servers_per_dc - 1))); do
          ipj_ib0=${ips[startj + j]}
          echo "DC$dci client $i ($ipi_ip) -- DC$dcj server $j ($ipj_ib0)"
          ssh $ipi_ip "$(typeset -f getifname); sudo tc filter add dev \$(getifname) parent 1:0 protocol ip pref 55 handle ::55 u32 match ip dst $ipj_ib0 flowid 2:1;"
        done
      done

      for i in $(seq 0 $(($num_servers_per_dc - 1))); do
        ipi_ip=${ips[starti + i]}
        for j in $(seq 0 $(($num_clients_per_dc - 1))); do
          ipj_ib0=${client_ips[startj + j]}
          echo $(getent hosts $ipj_ib0 | awk -F" " '{print $1}')
          echo "DC$dci server $i ($ipi_ip) -- DC$dcj client $j ($ipj_ib0)"
          ssh $ipi_ip "$(typeset -f getifname); sudo tc filter add dev \$(getifname) parent 1:0 protocol ip pref 55 handle ::55 u32 match ip dst $(getent hosts $ipj_ib0 | awk -F" " '{print $1}') flowid 2:1;"
        done
      done

    done
  done
fi
