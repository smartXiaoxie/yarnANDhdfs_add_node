#!/bin/bash
# 同步hosts 文件和yarn 文件
redis_cmd="/home/spark/anaconda2/bin/redis-cli -h 10.10.73.23 -p 6379"
nodemanager_file="/usr/local/hadoop/etc/hadoop/yarn-site_nodemanager.xml"
hosts_file="/etc/hosts_yarn"
local_file="/etc/hosts"
start_cmd="/usr/local/hadoop/sbin/yarn-daemon.sh"

# 取临时成功的 节点，同步hosts 文件
# 取临时成功的节点
for i in `$redis_cmd  smembers temp_nodemanager`
do
   i_ip=`echo $i | awk -F':' '{print $1}'`
   i_id=`echo $i | awk -F':' '{print $2}'`
   echo "$i_ip $i_id" >> ${hosts_file}
   echo "$i_ip $i_id" >> ${local_hosts}
done

# 同步yarn 节点的hosts 文件
for host in `$redis_cmd smembers yarn_cluster`
do
   scp ${hosts_file} root@${host}:/etc/hosts
done

# 同步到其他的客户机上
for client_host in  master_standby workstation hue01
do
    scp ${hosts_file} root@${client_host}:/etc/hosts
done

# 同步hosts和yarn 文件到新增的节点上
for new_host in `$redis_cmd smembers temp_nodemanager`
do
   ip=`echo $new_host | awk -F':' '{print $1}'`
   scp -o strictHostkeychecking=no ${hosts_file} root@${ip}:/etc/hosts
   scp -o strictHostkeychecking=no ${namemanager_file} spark@${ip}:/usr/local/hadoop/etc/hadoop/yarn_site.xml
   ssh -o ConnectTimeout=10 -o strictHostkeychecking=no spark@${ip} "${start_cmd} start nodemanager"
done
sleep 10

# 检查nodemanager 是否启动成功
for check_host in `$redis_cmd smembers temp_nodemanager`
do
   check_ip=`echo ${check_host} | awk -F':' '{print $1}'`
   check_id=`echo ${check_host} | awk -F':' '{print $2}'`
   stat=`ssh spark@${check_ip} -o strictHostkeychecking=no "/usr/local/jdk/bin/jps | grep 'NodeManager' | wc -l "`
   if [ $stat == 0 ]
   then
       $redis_cmd sadd temp_mv_nodemanager ${check_ip}
       $redis_cmd srem temp_nodemanager ${check_host}
   else
       echo " 启动成功"
       $redis_cmd sadd yarn_cluster ${check_id}
   fi
done
