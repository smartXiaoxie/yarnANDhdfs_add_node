#!/bin/bash
# 自动创建hostname 和 data需要相关的目录 
if [ -z  $1 ] 
then
   echo "no"
   exit 1
fi

hostname $1
sed "s/HOSTNAME=.*/HOSTNAME=$1/g" /etc/sysconfig/network

# 创建 /data 的目录
mkdir -p /data/hadoop/tmp/dfs/{data,name}
#配置 yarn的 
mkdir -p /data/hadoop/yarn/{local,log}
chmod -R 777 /data/hadoop
chown -R spark:spark /data/hadoop
