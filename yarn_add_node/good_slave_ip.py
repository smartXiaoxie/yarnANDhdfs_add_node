#coding=utf-8
import os
import datetime
import sys
import time
import user_auth
import subprocess
import json
import shutil

#将创建的slave经过判断是否能用保存到分别的文件中，good_slave 中存放启动好的主机，bad_slave 中存放不好的主机，
#在计算结束之后的删除程序会自动删除文件，不影响下次使用。
def good_slave(data):
    today=time.strftime('%Y-%m-%d',time.localtime())
    file="/data/spark"
    file_log=file+"/"+"good_slave"+".log"
    f=open(file_log,'a')
    line=data
    f.write(line + '\n')
    f.flush()
    f.close()

def bad_slave(data):
    today=time.strftime('%Y-%m-%d',time.localtime())
    file="/data/spark"
    file_log=file+"/"+"bad_slave"+".log"
    f=open(file_log,'a')
    line=data
    f.write(line + '\n')
    f.flush()
    f.close()

#最后用于查看添加成功的节点
def check_slave_num():
    good_file="/data/spark/good_slave.log"
    bad_file="/data/spark/bad_slave.log"
    if os.path.exists(good_file):
        good=open(good_file)
        all_good=good.read()
        print "添加成功的主机是："
        print all_good
        good.close()
    else:
        print "good_host is  none"
    if os.path.exists(bad_file):
        bad=open(bad_file)
        all_bad=bad.read()
        print "添加以下主机失败："
        print all_bad
        bad.close()
    else:
        print "bad_host is none"

# 储存临时文件和临时节点
def save_tmp_node(data):
    add_node = open("/home/spark/myfile/spark/yarn_add.txt",'a')
    tmp_file = open("/home/spark/myfile/spark/yarn_tmp_hosts.txt",'a')
    host_data_tmp = data.replace(":"," ")
    tmp_file.write(host_data_tmp + '\n')
    add_node.write(data + '\n')
    tmp_file.flush()
    add_node.flush()
    tmp_file.close()
    add_node.close()

#定义执行命令函数
def exe_cmd(command, timeout=300):
    import subprocess, datetime, os, time, signal
    #cmd = command.split(" ")
    start = datetime.datetime.now()
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while process.poll() is None:
        time.sleep(30)
        now = datetime.datetime.now()
        if (now - start).seconds> timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            return None
    return process.stdout.read()

# 将对应的好节点和bad 节点都追加到相应的文件中
def save_result_file(slave_host_list,slave_id_list):
    if os.path.exists("/home/spark/myfile/spark/yarn_tmp_success.txt"):
        count = len(open("/home/spark/myfile/spark/yarn_tmp_success.txt",'r').readlines())
        print count
        if count == 0:
            print "没有一个主机是成功创建的"
        else:
            good_file = open("/data/spark/good_slave.log",'a')
            yarn_success_file = open("/home/spark/myfile/spark/yarn_tmp_success.txt",'r')
            for line in yarn_success_file:
                data_dict = {}
                print line
                line = line.split('\n')[0]
                host_id = slave_id_list[slave_host_list.index(line)]
                print host_id
                data_dict = {line:host_id}
                print data_dict
                data_dict = json.dumps(data_dict)
                good_file.write(data_dict + '\n')
                good_file.flush()
            good_file.close()
            yarn_success_file.close()
            
        #os.remove("/home/spark/myfile/spark/yarn_tmp_success.txt")
    # 添加到 "/data/spark/dad_slave.log"
    # 查看失败的文件
    if os.path.exists("/home/spark/myfile/spark/yarn_tmp_reboot.txt"):
        count = len(open("/home/spark/myfile/spark/yarn_tmp_reboot.txt",'r').readlines())
        print count
        if count == 0:
            print "没有主机是失败的"
        else:
            bad_file = open("/data/spark/bad_slave.log",'a')
            yarn_fail_file = open("/home/spark/myfile/spark/yarn_tmp_reboot.txt",'r')
            for re_line in yarn_fail_file:
                data_dict = {}
                print re_line
                re_line = re_line.split('\n')[0]
                id = slave_id_list[slave_host_list.index(re_line)]
                print id
                data_dict = {re_line:id}
                print data_dict
                data_dict = json.dumps(data_dict)
                bad_file.write(data_dict + '\n')
                bad_file.flush()
            bad_file.close()
            yarn_fail_file.close()

# 用于删除节点的使用
def delete_slave_log():
    good_file="/data/spark/good_slave.log"
    if os.path.exists(good_file):
        os.remove(good_file)
        print "存储动态节点文件删除"
    
def delete_bad_log():
    bad_file="/data/spark/bad_slave.log"
    if os.path.exists(bad_file):
        os.remove(bad_file)

# 用于删除部分节点的使用，先将没有删除的主机写入bar 文件，再将bar 文件写到spark 文件，再删除bar
def part_del_node(name='0'):
    good_bar_file="/data/spark/good_slave_bar.log"
    good_file="/data/spark/good_slave.log"
    f_bar=open(good_bar_file,'a')
    f=open(good_file)
    line = 1
    for ip_data in f:
        if line > int(name):
            f_bar.write(ip_data)
        else:
            line +=1
    f_bar.flush()
    f_bar.close()
    f.close()
    if os.path.exists(good_bar_file):
        f_bar=open(good_bar_file)
        f=open(good_file,'w')
        for i in f_bar:
            f.write(i)
        f_bar.flush()
        f_bar.close()
        os.remove(good_bar_file)
        print "部分储存主机信息成功"
    else:
        print " 没有部分删除主机"

# 用于删除部分节点
def yarn_del_node(del_str):
    with open('/data/spark/good_slave.log','r') as f:
        with open('/data/spark/good_slave_bar.log','w') as g:
            for line in f.readlines():
                if del_str not in line:
                    g.write(line)
    shutil.move('/data/spark/good_slave_bar.log','/data/spark/good_slave.log')


#if __name__=='__main__':
    #part_del_node(3)
