# coding=utf-8
# 执行 ucoud api 申请资源

import hashlib
import json
import urllib
import urlparse
import requests

# 生成签名
def create_sig(private_key, params):
    items = params.items()
    # 请求参数串
    items.sort()
    # 将参数串排序

    params_data = "";
    for key, value in items:
        params_data = params_data + str(key) + str(value)
    params_data = params_data + private_key
    signature = hashlib.sha1(params_data).hexdigest()
    return signature
    # 生成的Signature值

# 创建spark-slave 节点的配置
def create_machine(Name = "yarn_node"):

    create_machine_cmd = {
        "Action": "CreateUHostInstance",
        "Region": "cn-bj2",
        "Zone": "cn-bj2-03",
        "ImageId": "uimage-edkh2g",
        "LoginMode": "Password",
        "Password": "",
        "CPU": "8",
        "Memory": "24576",
        "StorageType": "UDisk",
        "DiskSpace": "100",
        "ChargeType": "Dynamic",
        "BootDiskSpace": "20",
        "PublicKey": ""
    }

    create_machine_cmd["Name"] = Name
    return create_machine_cmd

# 执行主机关机操作
def stop_slave_host(host_id="uhost-1"):
    stop_slave_cmd={
        "Action":"StopUHostInstance",
        "Region":"cn-bj2",
        "Zone":"cn-bj2-03",
        "PublicKey":""
    }
    stop_slave_cmd["UHostId"]=host_id
    print stop_slave_cmd
    execute_cmd(stop_slave_cmd) 

# 执行api 的请求
def execute_cmd(cmd):
    base_url = "https://api.ucloud.cn/?"
    query_string = base_url + "&".join(["%s=%s" % (k, v) for k, v in cmd.items()])
    query_string
    private_key = ""    # api 的私钥
    signature = create_sig(private_key, cmd)
    query_string += "&Signature=" + signature
    response = requests.get(url=query_string)
    returnInfo = json.loads(response.text)
    return returnInfo

#这个是后期需要重启主机的函数，需得到uhost 的ID号
def reboot_slave(host_id="uhost-1"):
    reboot_slave_cmd={
        "Action":"RebootUHostInstance",
        "Region":"cn-bj2",
        "Zone":"cn-bj2-03",
        "PublicKey":""
    }
    reboot_slave_cmd["UHostId"]=host_id
    return reboot_slave_cmd

# 这个是删除节点主机使用
def delete_slave(host_id="uhost-1"):
    delete_slave_cmd={
        "Action":"TerminateUHostInstance",
        "Region":"cn-bj2",
        "Zone":"cn-bj2-03",
        "PublicKey": ""
    }
    delete_slave_cmd["UHostId"]=host_id
    return delete_slave_cmd
       

# 提取ip 列表
def main_process(slave_num=3):
    sum=0
    counter=1
    ip_list=[]
    id_list=[]
    while counter <= int(slave_num):
        create_machine_cmd = create_machine("spark_slave1")
        return_data=execute_cmd(create_machine_cmd)
        ip_unic_list=return_data["IPs"]
        id_unic_list=return_data["UHostIds"]
        ip=str(ip_unic_list[0])
        id=str(id_unic_list[0])
        ip_list.append(ip)
        id_list.append(id)
        counter += 1
    ip_id_zip=dict(zip(ip_list,id_list))
    return ip_id_zip
    # 在这里返回的是一个zip 格式的，在后期循环中需要将zip 的key 和values都拆开用

if __name__=="__main__":
    create_machine_cmd = create_machine("spark_slave1")
    return_data=execute_cmd(create_machine_cmd)
    print return_data
