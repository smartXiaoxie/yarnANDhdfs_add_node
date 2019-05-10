#coding:utf-8
#xiaoxie
import paramiko
import createMachine
import good_slave_ip
import time,json,os,datetime,getpass,signal
import redis
import user_auth
import subprocess
import set_fair_scheduler
import requests
from common_dingding import DingdingActiveApi

class CreatYarnNode(object):
    def __init__(self):
        pass

    def slave_connect(self,host,seq):
        self.host = host
        self.seq = seq
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(self.host,username='root',allow_agent=True,look_for_keys=True)
        except:
            return None
        #执行脚本
        cmd = "/bin/sh /root/auto_yarn_node.sh " + str(self.seq)
        stdin,stdout,stderr = ssh.exec_command(cmd)
        print stdout.read()
        print stderr.read()
        return True

    # 连接 redis
    def conn_redis(self):
        self.client = redis.Redis(host='10.10.73.23', port=6379)
        return self.client

    # 判断域名，保证唯一性
    def build_hosts(self,slave_host_list):
        hosts_count = 20
        node_cluster = self.client.smembers("yarn_cluster")
        for ip in slave_host_list:
            while True:
                hosts = "node" + str(hosts_count)
                if hosts not in node_cluster:
                    # 开始 主机自动进行创建主机name 和创建需要的目录
                    return_stat = self.slave_connect(ip, hosts_count)
                    if return_stat:
                        temp_nodemanager_line = str(ip) + ':' + str(hosts)
                        self.client.sadd('temp_nodemanager', temp_nodemanager_line)
                        hosts_count = hosts_count + 1
                        break
                    else:
                        self.client.sadd('temp_mv_nodemanager', ip)
                        break
                else:
                    hosts_count = hosts_count + 1

    def main(self,slave_need_num=0):
        # 获取yarn 现有的节点数
        url = "http://master:8088/ws/v1/cluster/metrics"
        data = requests.get(url)
        code = data.status_code
        if code != 200:
            exit()
        action_node = data.json()["clusterMetrics"]["activeNodes"]
        # 创建主机
        self.create_data = createMachine.main_process(slave_need_num)
        self.slave_host_list = self.create_data.keys()
        # 将主机的Ip和id 列表进行分开循环
        self.build_hosts(self.slave_host_list)
        time.sleep(120)
        if int(self.client.scard("temp_nodemanager")) != 0:
            action_cmd = "/bin/sh /home/spark/myfile/auto_create_yarn/rsync_nodemanager_hosts.sh "
            action_stat = os.system(action_cmd)
            if action_stat == 0:
                count = int(self.client.scard("temp_nodemanager"))
                if count != 0:
                    print self.client.smembers("temp_nodemanager")
                    # 配置 yarn 的任务队列
                    fair_file = "/usr/local/hadoop/etc/hadoop/fair-scheduler.xml"   # 写入调度的配置文件
                    dingding_message = DingdingActiveApi()
                    url = ""   # 发送到dingding 的群url
                    fair_status = set_fair_scheduler.Set_fair(fair_file,count, action_node)
                    if fair_status:
                        fair_line = "yarn 自动添加节点配置队列成功\n" + "添加节点数:" + str(count)
                        dingding_message.push_dingding_message(url, "text", fair_line, "电话号码")
                    else:
                        dingding_message.push_dingding_message(url, "text",
                                                               "yarn自动添加节点配置fair-scheduler文件失败,请手动配置\n" + "处理人:人名",
                                                               "号码")
            else:
                 print "配置脚本执行失败"
        else:
            print " 没有新增节点连接成功"
        #将节点状态信息进行更新
        if int(self.client.scard("temp_nodemanager")) != 0:
            # 将 temp 临时集合的节点放入到yarn 集群集合和临时存放新增节点的 集合中
            for hosts in self.client.smembers("temp_nodemanager"):
                node_name = hosts.split(':')[1]
                node_ip = hosts.split(':')[0]
                self.client.sadd("yarn_cluster",node_name)
                node_info = str(hosts) + str(self.create_data[node_ip])
                self.client.sadd("yarn_add_node",node_info)
        self.client.delete("temp_nodemanager")

        # 将失败的节点进行删除,以及去除hosts 文件的内容
        if int(self.client.scard("temp_mv_nodemanager")) != 0:
            delete_cmd = "/bin/sh /home/spark/myfile/auto_create_yarn/new_yarn_tmp_reboot.sh"
            for mv_ip in self.client.smembers("temp_mv_nodemanager"):
                stop_result = createMachine.stop_slave_host(self.create_data[mv_ip])
                delete_cmd = createMachine.delete_slave(self.create_data[mv_ip])
                return_data = createMachine.execute_cmd(delete_cmd)
                returncode = return_data["RetCode"]
                if int(returncode) == 0:
                    print "删除节点 %s 成功" % ip
                    log_data = "apply error nodes already  be deleted"
                    user_auth.logging(log_data)
        self.client.delete("temp_mv_nodemanager")




