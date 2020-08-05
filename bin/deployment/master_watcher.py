# coding=utf-8
# 使用说明：
# Master的watcher脚本，用过类crontab逻辑来管理，不添加long running逻辑
# 输入的master和worker部署节点列表，需要是salt能识别的id，测试使用的是ip

import traceback
import os
import sys
import random
import subprocess
import socket
import time

MASTER_ADDRESS_FILE = 'master_address'
SUCCESS_FILE = '_SUCCESS'
MASTER_CHECKER_PATH = '/ess_master_address'
RESTART = 'restart'
CHECK = 'check'
BOOTSTRAP = 'bootstrap'


# return master host
def select_master_host(master_list):
    random.shuffle(master_list)
    for host in master_list:
        if os.system("ping -c 1 " + host) == 0:
            return host
    return


def restart_cluster(master_list, worker_list, old_master, port):
    print("begin to restart whole cluster")
    old_master_host = ""
    if old_master != "":
        old_master_host = old_master.split(":")[0]
    try:
        os.system("hdfs dfs -rm {}/{}".format(MASTER_CHECKER_PATH, SUCCESS_FILE))
        master_node = select_master_host(master_list)
        if master_node == "":
            raise ValueError('there is no alive master node to start master service!')
        os.system('sh restart_cluster.sh {} {} {} {}'.format(master_node, port, worker_list, old_master_host))
        # write master address
        os.system('echo {}:{} > {} | hdfs dfs -put -f {} {}'.format(master_node, port, MASTER_ADDRESS_FILE,
                                                              MASTER_ADDRESS_FILE, MASTER_CHECKER_PATH))

        # write success file
        os.system('touch {} | hdfs dfs -put -f {} {}'.format(SUCCESS_FILE, SUCCESS_FILE, MASTER_CHECKER_PATH))

        # clean
        os.system('rm {} | rm {}'.format(MASTER_ADDRESS_FILE, SUCCESS_FILE))
    except Exception, ex:
        print  traceback.format_exc()


def start_check(address):
    s = socket.socket()
    ip = address.split(":")[0]
    port = address.split(":")[1]
    print "attempting to connect to %s on port %s" % (ip, port)
    for _ in range(3):
        try:
            s.connect((ip, int(port)))
            print "connected"
            return True
        except socket.error, e:
            print "failed"
        time.sleep(3)
    return False


def main(argv):
    from optparse import OptionParser
    parser = OptionParser()

    print "example:{}".format("python master_watcher.py -o check -m 192.168.6.85 -p 9099 -w 192.168.6.85,192.168.6.86")
    parser.add_option("-o", "--operation", dest="operation", default='check', help="OPERATION for this call",
                      metavar="OPERATION")
    parser.add_option("-m", "--masterList", dest="masterList", help="node list to deploy master", metavar="masterlist")
    parser.add_option("-p", "--port", dest="port", type="int", default=80, help="PORT for server", metavar="PORT")
    parser.add_option("-w", "--workerList", dest="workerList", help="node list to deploy workers", metavar="workerlist")

    (options, args) = parser.parse_args()
    print 'options %s ,args %s' % (options, args)
    operation = options.operation
    master_list = options.masterList.split(",")
    port = options.port
    worker_list = options.workerList

    print("begin to run master watcher")
    try:
        if operation == BOOTSTRAP:
            os.system("hdfs dfs -mkdir {}".format(MASTER_CHECKER_PATH))
            restart_cluster(master_list, worker_list, "", port)
        else:
            proc = subprocess.Popen("hdfs dfs -ls {}".format(MASTER_CHECKER_PATH) +
                                    "| awk '{print $8}' | awk -F'/' '{print $NF}'", stdout=subprocess.PIPE, shell=True)
            master_address_files = proc.stdout.read()
            has_success = False if SUCCESS_FILE not in master_address_files else True
            print "success file status {}".format(has_success)
            if has_success:
                proc = subprocess.Popen("hdfs dfs -cat {}/{}".format(MASTER_CHECKER_PATH, MASTER_ADDRESS_FILE),
                                        stdout=subprocess.PIPE, shell=True)
                address = proc.stdout.read()
                print "current master address {}".format(address)
                master_alive = start_check(address)
                if not master_alive or operation == RESTART:
                    restart_cluster(master_list, worker_list, address, port)
    except Exception, ex:
        print  traceback.format_exc()


if __name__ == "__main__":
    main(sys.argv)
