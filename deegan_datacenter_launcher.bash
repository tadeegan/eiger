#!/bin/bash
#
# Launches a datacenter on this machine.
#
# Requires: add_loopback_address.bash

./kill_all_cassandra.bash
source deegan_env.sh
export max_mutation_delay_ms=$1
echo $max_mutation_delay_ms
set -u

#this file name is hardcoded into cassandra ... I'll work with it for now
topo_file=conf/digital-ocean-topology.properties
cp $topo_file conf/cassandra-topology.properties 
ips='104.236.140.240, 188.226.251.145, 104.236.191.32'

#remove old log files
rm cassandra_var/cassandra*log

#clean out data directories
src_dir=$(pwd)
#setup

mkdir ${src_dir}/cassandra_var/data 2> /dev/null
mkdir ${src_dir}/cassandra_var/commitlog 2> /dev/null
mkdir ${src_dir}/cassandra_var/saved_caches 2> /dev/null
mkdir ${src_dir}/cassandra_var/stdout 2> /dev/null

rm -rf ${src_dir}/cassandra_var/data/* 2> /dev/null
rm -rf ${src_dir}/cassandra_var/commitlog/* 2> /dev/null
rm -rf ${src_dir}/cassandra_var/saved_caches/* 2> /dev/null

global_node_num=0
mkdir ${src_dir}/cassandra_var/data/$global_node_num
mkdir ${src_dir}/cassandra_var/commitlog/$global_node_num
mkdir ${src_dir}/cassandra_var/saved_caches/$global_node_num

conf_file=eiger_conf.yaml
log4j_file=log4j-server_${global_node_num}.properties

#create the custom config file for this node
sed 's/INITIAL_TOKEN/'$local_token'/g' conf/cassandra_BASE.yaml \
    | sed 's/SEEDS/'"$ips"'/g' \
    | sed 's/LISTEN_ADDRESS/'$local_ip'/g' \
    | sed 's/RPC_ADDRESS/'$local_ip'/g' \
    | sed 's/NODE_NUM/'$global_node_num'/g' \
    > conf/$conf_file

sed 's/LOG_FILE/cassandra_var\/cassandra_system.'$global_node_num'.log/g' conf/log4j-server_BASE.properties > conf/$log4j_file

#Want small JVM mem sizes so this can all run on one machine
export JVM_OPTS="-Xms32M -Xmn64M"

out_file=${src_dir}/cassandra_var/stdout/eiger.out
touch out_file

set -x
bin/cassandra -Dcassandra.config=${conf_file} -Dcom.sun.management.jmxremote.port=$((7199 + global_node_num)) -Dlog4j.configuration=${log4j_file} > ${out_file}
set +x
#wait until all nodes have joined the ring
normal_nodes=0
echo "Nodes up and normal: "

if [ $# -gt 1 ]; then
    exit
fi
num_nodes=$(wc -l <  conf/digital-ocean-topology.properties)
while [ "${normal_nodes}" -ne $num_nodes ]; do
    sleep 5
    normal_nodes=$(bin/nodetool -h 127.0.0.1 ring 2>&1 | grep "Normal" | wc -l)
    echo "normal nodes "$normal_nodes
done
