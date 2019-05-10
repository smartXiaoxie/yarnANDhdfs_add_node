#coding:utf-8
# 用于自动添加 hadoop 节点使用,hadoop1 和hadoop2 集群都使用
import redis
import paramiko
import createMachine
import json
import signal
import sys
import time
import os

def create_cluster1_cmd(Name = "hadoop1_datanode"):
    create_machine_cmd = {
        "Action": "CreateUHostInstance",
        "Region": "cn-bj2",
        "Zone": "cn-bj2-03",
        "ImageId": "uimage-vdbja5",
        "LoginMode": "Password",
        "Password": "",    # 写入加密的密码
        "CPU": "2",
        "Memory": "4096",
        "StorageType": "LocalDisk",
        "DiskSpace": "2000",
        "ChargeType": "Month",
        "BootDiskSpace": "20",
        "PublicKey": ""   # api  公钥
    }

    create_machine_cmd["Name"] = Name
    #print create_machine_cmd

    return create_machine_cmd


def create_cluster2_cmd(Name = "hadoop2_datanode"):
    create_machine_cmd = {
        "Action": "CreateUHostInstance",
        "Region": "cn-bj2",
        "Zone": "cn-bj2-02",
        "ImageId": "uimage-mdkauz",
        "LoginMode": "Password",
        "Password": "",
        "CPU": "1",
        "Memory": "2048",
        "StorageType": "UDisk",
        "DiskSpace": "3000",
        "ChargeType": "Month",
        "BootDiskSpace": "20",
        "PublicKey": ""
    }

    create_machine_cmd["Name"] = Name
    return create_machine_cmd

def create_datanode(cluster_type='none',slave_num='0'):
    sum = 0 
    count = 1
    ip_list = []
    id_list = []
    if cluster_type == "hadoop1":
        create_cmd = create_cluster1_cmd("hadoop1_datanode")
    elif cluster_type == "hadoop2":
         create_cmd = create_cluster2_cmd("hadoop2_datanode")
    else:
         exit()
    #返回机器数
    while count <= int(slave_num):
        return_data = createMachine.execute_cmd(create_cmd)
        ip_unic_list=return_data["IPs"]
        id_unic_list=return_data["UHostIds"]
        ip=str(ip_unic_list[0])
        id=str(id_unic_list[0])
        ip_list.append(ip)
        id_list.append(id)
        count += 1
    ip_id_zip = dict(zip(ip_list,id_list))
    return ip_id_zip
        
# 连接 redis
def conn_redis():
    try:
        client = redis.Redis(host='10.10.73.23',port=6379)
        return client
    except:
        return None

# 连接host
def conn_host(host,work_hosts):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host,username='root',allow_agent=True,look_for_keys=True)
    except:
        return None
    # 执行脚本
    cmd1 = "/usr/bin/scp -o strictHostkeychecking=no /home/spark/myfile/audo_create_datanode/datanode_set_hosts.sh root@" + str(host) + ":/root/"
    stat = os.system(cmd1)
    if stat == 0:
        cmd = "/bin/sh /root/datanode_set_hosts.sh " + str(work_hosts)
        stdin,stdout,stderr = ssh.exec_command(cmd)
        return "yes"
    else:
         return None

if __name__ == '__main__':
    cluster_type = sys.argv[1]
    if cluster_type == "":
        print " 请输入是哪个集群添加节点, hadoop1 和 hadoop2"
        exit()
    slave_need_num = int(raw_input("please enter how many nodes you need:"))
    create_data = create_datanode(cluster_type,slave_need_num)
    slave_host_list = create_data.keys()
    slave_id_list = create_data.values()
    # 连接redis 
    client = conn_redis()
    if client:
        node_cluster = list(client.smembers(cluster_type))
    else:
         exit()
    time.sleep(300)
    if cluster_type == "hadoop1":
        work_hosts = "work"
    elif cluster_type == "hadoop2":
        work_hosts = "work2_"
        exit()
    hosts_count = 10
    client.delete('temp_datanode')
    client.delete('temp_mv_datanode')
    for ip in  slave_host_list:
        while True:
           hosts = ""
           hosts = str(work_hosts) + str(hosts_count)
           if hosts not in node_cluster:
                # 开始 主机自动进行创建主机name 和创建需要的目录
                return_stat = conn_host(ip,hosts)
                if return_stat:
                    temp_datanode_line = str(ip) +':' + str(hosts)
                    client.sadd('temp_datanode',temp_datanode_line)
                    hosts_count = hosts_count + 1
                    break
                else:
                    client.sadd('temp_mv_datanode',ip)
                    break
           else:
                hosts_count = hosts_count + 1
    if int(client.scard("temp_datanode")) != 0:
        action_cmd = "/bin/sh /home/spark/myfile/audo_create_datanode/rsync_cluster_hosts.sh " + str(cluster_type)
        action_stat = os.system(action_cmd)
        if action_stat == 0:
            if int(client.scard("temp_datanode")) != 0:
                client.delete("temp_datanode")
    else:
        print "没有成功的节点"
    if int(client.scard("temp_mv_datanode")) != 0:
        print " 请查看没有启动datanode成功的节点,不需要尽快删除"
        print client.smembers("temp_mv_datanode")
