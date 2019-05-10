#coding:utf-8
# 动态删除节点
import createMachine
import paramiko
import good_slave_ip
import os
import time
import user_auth
import getpass
import datetime
import subprocess
import set_fair_scheduler
from common_dingding import DingdingActiveApi

class DeleteYarnNode(object):
    def __init__(self):
        pass

    def reconnect_slave(self,host="10.10.10.10"):
        ssh=paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
           ssh.connect(host,username='spark',allow_agent=True,look_for_keys=True)
           return True
        except:
           return None

    def exe_cmd(self,command, timeout=300):
        start = datetime.datetime.now()
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while process.poll() is None:
            time.sleep(30)
            now = datetime.datetime.now()
            if (now - start).seconds> timeout:
                os.kill(process.pid, signal.SIGKILL)
                os.waitpid(-1, os.WNOHANG)
                return None
        print  process.stdout.read()

    # 连接 redis
    def conn_redis(self):
        self.client = redis.Redis(host='10.10.73.23', port=6379)
        return self.client

    def main(self,delete_node=0):
        now_add = int(self.client.scard("yarn_add_node"))
        if now_add == 0:
            print " 没有节点删除，将退出"
            exit()
        if int(delete_node) >= int(now_add):
            print "将删除全部"
            delete_node = int(now_add)
        node = 1
        delete_list = []
        while node < delete_node:
            delete_node_data = self.client.spop("yarn_add_node")
            self.client.sadd("yarn_delete_temp",delete_node_data)
            delete_list.append(delete_node_data)
            node = node + 1
        # 停止主机并删除hosts 文件
        self.exe_cmd("/bin/sh /home/spark/auto_create_yarn/new_rsync_hosts_rm.sh")
        #删除主机
        rm_node = 0
        for i in delete_list:
            ucloud_id = i.split(':')[2]
            createMachine.stop_slave_host(ucloud_id)
        time.sleep(120)
        for i in delete_list:
            ucloud_id = i.split(':')[2]
            delete_cmd = createMachine.delete_slave(ucloud_id)
            return_data = createMachine.execute_cmd(delete_cmd)
            returncode = return_data["RetCode"]
            if int(returncode) == 0:
                rm_node = rm_node + 1
            else:
                print " 没有被删除，该节点%s" % i
        # 减除配置
        dingding_message = DingdingActiveApi()
        url = ""    # 钉钉预警的url
        fair_file = "/usr/local/hadoop/etc/hadoop/fair-scheduler.xml"
        fair_status = set_fair_scheduler.Set_del_fair(fair_file, rm_node)
        if fair_status:
            fair_line = "yarn 自动删除节点配置队列成功\n" +  '\n' + "删除节点数:" + str(rm_node)
            dingding_message.push_dingding_message(url, "text", fair_line, "电话号码")
        else:
            dingding_message.push_dingding_message(url, "text",
                                                    "yarn 自动删除节点配置 fair-scheduler 文件失败，请手动配置\n" + "处理人:用户名",
                                                    "电话号码")
        # 删除一下记录
        for value in self.client.smembers("yarn_delete_temp"):
            self.client.srem("yarn_add_node",value)
            node_hosts = value.split(':')[1]
            self.client.srem("yarn_cluster",node_hosts)
        self.client.delete("yarn_delete_temp")









