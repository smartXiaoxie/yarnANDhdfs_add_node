#!/bin/bash
# 同步集群中所有的节点的hosts 
hadoop1_file="/etc/hosts_yarn"
hadoop2_file="/etc/hosts_hadoop2"
redis_cmd="/home/spark/anaconda2/bin/redis-cli -h 10.10.73.23 -p 6379"

if [ -z $1 ]
then
     echo "no"
     exit 1
fi

# 判断配置文件
if [ $1  == "hadoop1" ]
then
    hadoop_slave_config="/usr/local/hadoop/etc/hadoop"
    need_file=${hadoop1_file}
    start_cmd="/usr/local/hadoop/sbin/hadoop-daemon.sh"
elif [ $1 == "hadoop2" ]
then
    hadoop_slave_config="/usr/local/cloudera/hadoop-2.6.0-cdh5.4.1/etc/hadoop"
    need_file=${hadoop2_file}
    start_cmd="/usr/local/cloudera/hadoop-2.6.0-cdh5.4.1/sbin/hadoop-daemon.sh"
else
    echo " 没有该 集群"
    for i_mv in `$redis_cmd smembers temp_datanode`
    do 
        mv_ip=`echo ${i_mv} | awk -F':' '{print $1}'`
        $redis_cmd sadd temp_mv_datanode $mv_ip
        $redis_cmd srem temp_datanode $mv_ip
    done
    exit 1
fi

# 取临时成功的节点
for i in `$redis_cmd  smembers temp_datanode`
do
   i_ip=`echo $i | awk -F':' '{print $1}'`
   i_id=`echo $i | awk -F':' '{print $2}'`
   echo "$i_ip $i_id" >> $hadoop1_file
   echo "$i_ip $i_id" >> $hadoop2_file
   echo "$i_ip $i_id" >> /etc/hosts
done

# 同步hadoop1 的节点的hosts 文件
for host in `$redis_cmd smembers hadoop1`
do
   scp $hadoop1_file root@${host}:/etc/hosts
done

# 同步 hadoop2 的节点的 hosts 文件
for host1 in `$redis_cmd smembers hadoop2`
do
   scp $hadoop2_file root@${host1}:/etc/hosts
done

# 同步master-standby 上的文件
scp /etc/hosts root@master-standby:/etc/hosts

# 同步 master2 上的文件
scp $hadoop2_file root@master2:/etc/hosts

# 同步节点并启动节点
for ip in `$redis_cmd  smembers temp_datanode`
do 
   ip_host=`echo  $ip | awk -F':' '{print $1}'`
   scp  -o strictHostkeychecking=no ${need_file} root@${ip_host}:/etc/hosts
   scp -o strictHostkeychecking=no ${hadoop_slave_config}/hdfs-site.xml spark@${ip_host}:${hadoop_slave_config}/
   scp -o strictHostkeychecking=no ${hadoop_slave_config}/core-site.xml spark@${ip_host}:${hadoop_slave_config}/
   ssh -o ConnectTimeout=10 -o strictHostkeychecking=no spark@${ip_host} "${start_cmd} start datanode"
   sleep 10 
   process_num=`ssh -o ConnectTimeout=10 -o strictHostkeychecking=no spark@${ip_host} "jps | grep 'DataNode' | wc -l"`
   if [ $process_num != 0 ]
   then 
       echo "启动成功"
   else
       $redis_cmd sadd temp_mv_datanode ${ip_host}
       $redis_cmd srem temp_datanode $ip 
   fi   
done

# 将成功的节点添加到 hadoop slaves 文件
if [ `$redis_cmd scard temp_datanode` != 0 ]
then
    for s_i in `$redis_cmd smembers temp_datanode`
    do
       echo "$s_i" | awk  -F':' '{print $2}' >>  ${hadoop_slave_config}/slaves
       if [ $1 == "hadoop2" ]
       then
          scp  -o strictHostkeychecking=no ${hadoop_slave_config}/slaves master2:${hadoop_slave_config}/slaves
       fi
       s_ip=`echo $s_i | awk -F':' '{print $1}'`
       scp -o strictHostkeychecking=no ${hadoop_slave_config}/slaves  spark@${s_ip}:${hadoop_slave_config}/slaves
       seccusse_host=`echo "$s_i" | awk -F':' '{print $2}'`
       $redis_cmd sadd $1 $seccusse_host
    done
    # 进行同步slave 
    for s_host in `$redis_cmd smembers $1`
    do 
        scp  -o  strictHostkeychecking=no ${hadoop_slave_config}/slaves  spark@${s_host}:${hadoop_slave_config}/slaves
    done
fi     
