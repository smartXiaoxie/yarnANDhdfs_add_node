#!/bin/bash
# 停止节点的程序
redis_cmd="/home/spark/anaconda2/bin/redis-cli -h 10.10.73.23 -p 6379"
bin_nodemanager="/usr/local/hadoop/sbin/yarn-daemon.sh"
hosts_file="/etc/hosts_yarn"
local_file="/etc/hosts"

for i in `$redis_cmd smembers yarn_delete_temp`
do
    ip=`echo $i | awk -F':' '{print $1}'`
    ssh spark@${ip} -o ConnectTimeout=5 -o strictHostkeychecking=no "/usr/local/hadoop/sbin/yarn-daemon.sh  stop nodemanager"
    sudo sed -i "/^$ip.*$/d" ${hosts_file}
    sudo sed -i "/^$ip.*$/d" ${local_file}
done

#同步yarn_cluster
for host in `$redis_cmd smembers yarn_cluster`
do
    scp ${hosts_file}  root@${host}:/etc/hosts
done
scp ${hosts_file} root@master_standby:/etc/hosts
scp ${hosts_file} root@hue01:/etc/hosts
scp ${hosts_file} root@workstation:/etc/hosts