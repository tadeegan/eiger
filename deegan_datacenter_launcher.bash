#!/bin/bash
#
# Launches a datacenter on this machine.
#
# Requires: add_loopback_address.bash

./kill_all_cassandra.bash

set -u

num_dcs=1
nodes_per_dc=1
total_nodes=$((num_dcs * nodes_per_dc))

#sanity check
if [[ $total_nodes -gt 100 ]]; then
    echo "too many nodes, 100 max"
    exit
fi

echo "Checking loopback addresses"
#ensure we have the local ip address for each node (cassandra requires a unique ip per node)
local_addresses=$(ifconfig | grep 127.0.0 | grep "netmask 0xff000000" | wc -l)
if [[ $local_addresses -lt $total_nodes ]]; then
    echo "Adding loopback addresses! (requires sudo)"
    ./add_loopback_address.bash $((total_nodes - 1))
fi

#this file name is hardcoded into cassandra ... I'll work with it for now
topo_file=conf/cassandra-topology.properties

#remove old log files
rm cassandra_var/cassandra*log

#clean out data directories
src_dir=$(pwd)
rm -rf ${src_dir}/cassandra_var/data/* 2> /dev/null
rm -rf ${src_dir}/cassandra_var/commitlog/* 2> /dev/null
rm -rf ${src_dir}/cassandra_var/saved_caches/* 2> /dev/null

for dc in $(seq 0 $((num_dcs - 1))); do
    for n in $(seq 0 $((nodes_per_dc - 1))); do

        global_node_num=$((dc * nodes_per_dc + n))
        # tokens can't be identical even though we want them to be ... so for now let's get them as close as possible
        token=$(echo "${n}*(2^127)/${nodes_per_dc} + $dc" | bc)
        # Using tokens for evenly splitting type 4 uuids now
        #token=$(./uuid_token.py ${dc} ${n} ${nodes_per_dc})
        unset local_ip
        seeds=""
        for i in $(seq 0 $((total_nodes - 1))); do
            if [[ $i -eq $global_node_num ]]; then
                local_ip="127.0.0.$((i + 1))"
            fi
            seeds=$(echo $seeds"127.0.0.$((i + 1)), ")
        done
        echo $token" @ "$local_ip
        #echo $seeds

        echo "wassup"
        mkdir ${src_dir}/cassandra_var/data/$global_node_num
        mkdir ${src_dir}/cassandra_var/commitlog/$global_node_num
        mkdir ${src_dir}/cassandra_var/saved_caches/$global_node_num

        conf_file=${num_dcs}x${nodes_per_dc}_${dc}_${n}.yaml
        log4j_file=log4j-server_${global_node_num}.properties


        #create the custom config file for this node
        sed 's/INITIAL_TOKEN/'$token'/g' conf/cassandra_BASE.yaml \
            | sed 's/SEEDS/'"$seeds"'/g' \
            | sed 's/LISTEN_ADDRESS/'$local_ip'/g' \
            | sed 's/RPC_ADDRESS/'$local_ip'/g' \
            | sed 's/NODE_NUM/'$global_node_num'/g' \
            > conf/$conf_file

        sed 's/LOG_FILE/cassandra_var\/cassandra_system.'$global_node_num'.log/g' conf/log4j-server_BASE.properties > conf/$log4j_file

        #Want small JVM mem sizes so this can all run on one machine
        export JVM_OPTS="-Xms32M -Xmn64M"

        set -x
        bin/cassandra -Dcassandra.config=${conf_file} -Dcom.sun.management.jmxremote.port=$((7199 + global_node_num)) -Dlog4j.configuration=${log4j_file} > ${src_dir}/cassandra_var/stdout/${dc}_${n}.out
        set +x
    done
done

#wait until all nodes have joined the ring
normal_nodes=0
echo "Nodes up and normal: "
while [ "${normal_nodes}" -ne "${total_nodes}" ]; do
    sleep 5
    normal_nodes=$(bin/nodetool -h 127.0.0.1 ring 2>&1 | grep "Normal" | wc -l)
    echo "normal nodes "$normal_nodes
done
sleep 5
