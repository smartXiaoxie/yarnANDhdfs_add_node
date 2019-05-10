#coding:utf-8
#设置scheduler 文件资源
import subprocess
import os
import json
import requests
import re

# 根据节点的数量进行设置，默认是13个节点
def get_fair_source(file):
    if os.path.exists(file):
        queue_dict = {}
        f = open(file,'r')
        # 处理文件
        for line in f:
            if 'queue name=' in line and 'queue name="root"' not in line:
                queue_name = line.split('=')[1].split('>')[0].replace('"',"")
                queue_dict[queue_name] = {}
            elif '<minResources>' in line:
                minmb = line.split('mb')[0].split('>')[1].replace(' ','')
                mincore = line.split(',')[1].split('vcores')[0].replace(' ','')
                queue_dict[queue_name]["min"] = {}
                queue_dict[queue_name]["min"]["mb"] = minmb
                queue_dict[queue_name]["min"]["core"] = mincore
            elif '<maxResources>' in line:
                maxmb = line.split('mb')[0].split('>')[1].replace(' ','')
                maxcore = line.split(',')[1].split('vcores')[0].replace(' ','')
                queue_dict[queue_name]["max"] = {}
                queue_dict[queue_name]["max"]["mb"] = maxmb
                queue_dict[queue_name]["max"]["core"] = maxcore
        f.close()
        return queue_dict
    else:
        print "没有该文件,将退出"
         
# 替换文件中的内容
def file_sub(file,old_str,new_str):
    with open(file,"r") as f1,open("%s.bak" % file,"w") as f2:
        for line in f1:
            f2.write(re.sub(old_str,new_str,line))
    os.remove(file)
    os.rename("%s.bak" % file,file)

# 获取现有的节点数，判断fair 是否配置正常
def Set_fair(file,time=0,action_node=13):
    if int(time) == 0:
        print "没有新增节点数，将退出配置"
        exit()
    # 获取现有的节点数：
    normal_mb = int(action_node) * 20 * 1024
    normal_core = int(action_node) * 6
    now_queue_dict = get_fair_source(file)
    default_max_mb = now_queue_dict["default"]["max"]["mb"]
    default_max_core = now_queue_dict["default"]["max"]["core"]
    other_min_mb = 0
    other_min_core = 0
    for key in now_queue_dict.keys():
        if key != "default":
            other_min_mb = other_min_mb + int(now_queue_dict[key]["min"]["mb"])
            other_min_core = other_min_core + int(now_queue_dict[key]["min"]["core"])
    now_queue_core = int(default_max_core) + other_min_core
    now_queue_mb = int(default_max_mb) + other_min_mb
    if int(normal_core) == int(now_queue_core) and int(normal_mb) == int(now_queue_mb):
        print " fair 配置正常"
    else:
        print " fair 配置不正常，请使用手动配置"
        return None
    # 分情况进行配置
    add_default_mb = int(time) * 20 * 1024 + int(default_max_mb)
    add_default_core = int(time) * 6 + int(default_max_core)
    # 修改fair 文件的配置
    file_sub(file,str(default_max_core),str(add_default_core))
    file_sub(file,str(default_max_mb),str(add_default_mb))
    if os.path.exists(file):
        status = os.system('/usr/local/hadoop/bin/yarn rmadmin -refreshQueues')
        if status == 0:
            print "调度配置刷新成功"
            return True
        else:
            print "调度配置刷新失败"
            return None

def Set_del_fair(file,time=0):
    if int(time) == 0:
        print "没有新增节点数，将退出配置"
        exit()
    now_queue_dict = get_fair_source(file)
    default_max_mb = now_queue_dict["default"]["max"]["mb"]
    default_max_core = now_queue_dict["default"]["max"]["core"]
    if int(time) == 0:
        print "没有将减少节点的资源，将退出"
        exit()
    del_queue_mb = int(default_max_mb) - int(time) * 20 * 1024
    del_queue_core = int(default_max_core) - int(time) * 6
    if del_queue_mb >= 204800 and del_queue_core >= 60:
        file_sub(file, str(default_max_core), str(del_queue_core))
        file_sub(file, str(default_max_mb), str(del_queue_mb))
    else:
        print "配置错误，请手动配置，将退出"
        return None
    if os.path.exists(file):
        status = os.system('/usr/local/hadoop/bin/yarn rmadmin -refreshQueues')
        if status == 0:
            print "调度配置刷新成功"
            return True
        else:
            print "调度配置刷新失败"
            return None


