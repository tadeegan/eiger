./kill_all_cassandra.bash
src_dir=$(pwd)
rm -rf ${src_dir}/cassandra_var/data/* 2> /dev/null
rm -rf ${src_dir}/cassandra_var/commitlog/* 2> /dev/null
rm -rf ${src_dir}/cassandra_var/saved_caches/* 2> /dev/null
