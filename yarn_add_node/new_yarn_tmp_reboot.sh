#!/bin/bash
# 删除一些没有成功的节点
#xiaoxie
redis_cmd="/home/spark/anaconda2/bin/redis-cli -h 10.10.73.23 -p 6379"
local_file="/etc/hosts"
hosts_file="/etc/hosts_yarn"

# 删除hosts 文件
if `$redis_cmd scrad temp_mv_nodemanager` != 0
then
    for ip  in `$redis_cmd smembers temp_mv_nodemanager`
    do
        sudo sed -i "/^$i.*$/d" ${hosts_file}
        sudo sed -i "/^$i.*$/d" ${local_file}
    done
    # 同步hosts 文件
    for i in `$redis_cmd smembers yarn_cluster`
    do
       scp ${hosts_file} root@${i}:/etc/hosts
    done
    scp ${hosts_yarn} root@master-standby:/etc/hosts
    scp ${hosts_yarn} root@workstation:/etc/hosts
    scp ${hosts_yarn} root@hue01:/etc/hosts
fi
