#/usr/bin/env python
#coding:utf-8
from pymongo import MongoClient
import base64
import datetime
import os
import time

def conn_mongo():
    client=MongoClient('mongodb://')
    collection=client.users["user"]
    return collection

# 用于用户的认证
def user_auth(username,password):
    client = conn_mongo()
    cursor = client.find({"username":username})
    if cursor.count() == 0:
        return "not"
    else:
        encode_pass = base64.encodestring(password).replace('\n', '')
        for data in cursor:
            pass_data = str(data["password"])
    if str(encode_pass) == str(pass_data):
        return "yes"
    else:
        return "no"

def logging(data):
    logfile = "/usr/local/zabbix/script/spark_yarn/add_spark_node.log"
    log_f = open(logfile,'a')
    today_time=time.strftime('%Y-%m-%d %X',time.localtime())
    lines = " %s : %s " % (today_time,data)
    log_f.write(lines + '\n')
    log_f.flush()
    log_f.close()

if __name__=='__main__':
    print "auto add spark action slave"
    apply_user = raw_input("please enter your name:")
    log_data = "apply to add  spark node by user %s" % apply_user
    logging(log_data)
    if apply_user == "":
        print "please enter user name to processd"
        log_data = "not enter username,process error"
        logging(log_data)
        exit()
    else:
        time_num = 1
        while True:
            if time_num <= 3:
                apply_passwd = raw_input("please enter username password:")
                time_num = time_num + 1
                if apply_passwd == "":
                    print "password error"
                    print "The remaining %d inputs" % (3 - time_num)
                    log_data = "apply user %s  password auth failuer" % apply_user
                    logging(log_data)
                    continue
                else:
                     status = user_auth(apply_user,apply_passwd)
                     print status
                     if status == "not":
                         log_data = "apply user %s not privileges" % apply_user
                         logging(log_data)
                         print "without the user,the user "
                         exit()
                     elif status == "yes":
                         log_data = "apply user %s auth secusseful" % apply_user
                         logging(log_data)
                         print "auth secussful"
                         break
                     elif status == "no":
                         log_data = "apply user %s auth failuer" % apply_user
                         logging(log_data)
                         print "auth failure, password incorrect, please re-enter"
                         print "The remaining %d inputs" % (3 - time_num)
                         continue
            else:
                print " auth  failuer"
                exit()
