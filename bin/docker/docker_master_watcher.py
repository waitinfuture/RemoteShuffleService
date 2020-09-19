# coding=utf-8
# 使用说明：
# Master的watcher脚本，用过类crontab逻辑来管理，不添加long running逻辑
# 输入的master和worker部署节点列表，需要是salt能识别的id

# 选主逻辑：（具体在下面实现逻辑处也有注释）
# 1. 优先选择-m/--masterList 参数传递的master备选地址列表，如果启动失败，跳到2
# 2. 从workerList中，随机选择一个节点作为master节点

import sys
import random
import socket
import time
import subprocess, threading
import argparse

MASTER_ADDRESS_FILE = 'master_address'
SUCCESS_FILE = '_SUCCESS'
MASTER_CHECKER_PATH = '/ess_master_address'
CHECK = 'check'
BOOTSTRAP = 'bootstrap'

# 这个函数目的是支持shell命令超时逻辑
class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.outstr = ""
        self.rc = None

    def run(self, timeout):
        def target():
            print('Thread started')
            self.process = subprocess.Popen(self.cmd, shell=True, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
            output_str = list()
            while True:
                output = self.process.stdout.readline()
                if output == '' and self.process.poll() is not None:
                    break
                if output:
                    print(output.strip())
                    output_str.append(output.strip())
            self.rc = self.process.poll()
            self.outstr = ''.join(output_str)
            print('Thread finished')

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            print('Command {} process timeout , terminating process'.format(self.cmd))
            self.process.terminate()
            thread.join()
        return self.rc, self.outstr, ""


def execute_command_with_timeout(command, timeout=15):
    print("command: {}".format(command))
    command = Command(command)
    return Command.run(command, timeout)


def read_worker_list_file(filename):
    f = open(filename)
    content = f.readlines()
    f.close()
    content = [x.strip() for x in content]
    return ','.join(content)

def read_local_disk_volume_file(filename):
    f = open(filename)
    content = f.readlines()
    f.close()
    res = ""
    for x in content:
        disk = x.strip()
        res = "{} -v {}:{}".format(res, disk, disk)#res + " -v " + disk + ":" + disk
    return res

def restart_cluster(master_list, workers, port, local_disks, image_tag, worker_cpu_limit):
    print("begin to restart whole cluster, master_list is " + ",".join(master_list))
    worker_list = workers.split(",")

    # 这一段是启动集群的时候，master选主逻辑，具体如下：
    # 1. 优先选择-m/--masterList 参数传递的master备选地址列表，如果启动失败，跳到2
    # 2. 从workerList中，随机选择一个节点作为master节点
    diff = list(set(worker_list) - set(master_list))
    random.shuffle(diff)
    candidates=master_list + diff

    try:
        execute_command_with_timeout("hdfs dfs -rm {}/{}".format(MASTER_CHECKER_PATH, SUCCESS_FILE))
        for master_node in candidates:
            if master_node == "":
                continue
            master_node = master_node.split(":")[0]
            print("try start master on " + master_node)
            # 核心逻辑，操作restart_docker_cluster.sh脚本
            execute_command_with_timeout(
                'sh restart_docker_cluster.sh {} {} {} "{}" {} {} {}'.format(
                    master_node, port, workers, local_disks, image_tag, worker_cpu_limit, ','.join(candidates)), 300)
            if start_check("{}:{}".format(master_node, port), port):
                # write master address
                execute_command_with_timeout(
                    'echo {}:{} > {} | hdfs dfs -put -f {} {}'.format(master_node, port, MASTER_ADDRESS_FILE,
                                                                      MASTER_ADDRESS_FILE, MASTER_CHECKER_PATH))

                # write success file
                execute_command_with_timeout(
                    'touch {} | hdfs dfs -put -f {} {}'.format(SUCCESS_FILE, SUCCESS_FILE, MASTER_CHECKER_PATH))

                # clean
                execute_command_with_timeout('rm {} | rm {}'.format(MASTER_ADDRESS_FILE, SUCCESS_FILE))

                # return
                print("Successfully start master on " + master_node)
                return
            else:
                print("master process is not ready, try next node!")
                continue

        print("Failed to restart cluster!")
    except Exception as ex:
        print(ex)

def start_check(address, port):
    s = socket.socket()
    host = address.split(":")[0]
    if ":" in address:
        port = address.split(":")[1]
    print("attempting to connect to %s on port %s" % (host, port))
    for _ in range(3):
        try:
            s.connect((host, int(port)))
            print("connected")
            return True
        except socket.error as e:
            print(e)
        finally:
            s.close()
        time.sleep(3)
    return False

def get_master_addres_from_hdfs():
    _, master_address_files, _ = execute_command_with_timeout("hdfs dfs -ls {}".format(MASTER_CHECKER_PATH) +
                                                              "| awk '{print $8}' | awk -F'/' '{print $NF}'")
    has_success = False if SUCCESS_FILE not in master_address_files else True
    print("success file status {}".format(has_success))
    if has_success:
        _, address, _ = execute_command_with_timeout("hdfs dfs -cat {}/{}".format(
            MASTER_CHECKER_PATH, MASTER_ADDRESS_FILE))
        print("current master address {}".format(address))
        return address
    else:
        return ""

def main(argv):
    parser = argparse.ArgumentParser(description='Do master watcher...')

    print("Usage:{}".format("python docker_master_watcher.py -o bootstrap/check"
                              " -m 192.168.6.85 -p 9099 -f filename -d filename -i imageTag -C 8"))
    # 操作：支持bootstrap/check
    parser.add_argument("-o", "--operation", dest="operation", default='check', help="OPERATION for this call",
                      metavar="OPERATION", required=True)
    # master列表，逗号分隔，可为空
    parser.add_argument("-m", "--masterList", dest="masterList", help="node list to deploy master", metavar="masterlist", required=True)
    # master端口
    parser.add_argument("-p", "--port", dest="port", type=int, default=80, help="PORT for server", metavar="PORT", required=True)
    # worker地址列表文件路径，地址需要是salt的minion id
    parser.add_argument("-f", "--workerListFile", dest="workerListFile", help="worker nodes file",
                      metavar="workerListFile", required=True)
    # 启动worker container mount的volume，例如：-v /mnt/disk1:/mnt/disk1 -v /mnt/disk2:/mnt/disk2 -v /mnt/disk3:/mnt/disk3
    parser.add_argument("-d", "--diskVolumesFile", dest="diskVolumesFile", help="local disks volumes file",
                      metavar="diskVolumesFile", required=True)
    # 镜像tag，例如：registry.cn-beijing.aliyuncs.com/zf-spark/emr-shuffle-service:v1.0.0
    parser.add_argument("-i", "--imageTag", dest="imageTag", help="ess image tag",
                      metavar="imageTag", required=True)
    # worker container cpu limit
    parser.add_argument("-C", "--workerCpuLimit", dest="workerCpuLimit", help="worker container cpu limit",
                      metavar="workerCpuLimit", required=True)

    args = parser.parse_args()
    print('args %s' % args)
    operation = args.operation
    if args.masterList is None:
        master_list = []
    else:
        master_list = args.masterList.split(",")
    port = args.port
    worker_list = read_worker_list_file(args.workerListFile)
    local_disks = read_local_disk_volume_file(args.diskVolumesFile)
    image_tag = args.imageTag
    worker_cpu_limit = args.workerCpuLimit

    print("begin to run master watcher, operation " + operation)
    try:
        if operation == BOOTSTRAP:
            # 这个分支是重启/初始化集群。如果传递masterList为空并且集群已经初始化过，达到的目的就是重启集群
            execute_command_with_timeout("hdfs dfs -mkdir {}".format(MASTER_CHECKER_PATH))
            address_in_hdfs = get_master_addres_from_hdfs()
            if address_in_hdfs != "":
                master_list.append(address_in_hdfs)
            restart_cluster(master_list, worker_list, port, local_disks, image_tag, worker_cpu_limit)
        elif operation == CHECK:
            # 这个分支是探活逻辑，master地址支持从hdfs读取和参数指定两种方式，具体如下：
            # 1. 首先从hdfs读取master地址，并且添加到masterList，为空就忽略；
            # 2. 遍历masterList的所有地址进行探活，一旦发现有存活的地址就退出此次探活；
            # 注意：一般情况下，master_list为空即可，直接从hdfs中获取当前工作的master地址。
            #      如果该master地址无法连接（尝试3次），认为该master dead，会重启集群。
            addresses = set(master_list)
            address_in_hdfs = get_master_addres_from_hdfs()
            if address_in_hdfs != "":
                addresses.add(address_in_hdfs)

            for addr in addresses:
                if start_check(addr, port):
                    print("master alive on " + addr)
                    return
                else:
                    print("master NOT alive on " + addr + ", try next")

            print("no alive master! restart cluster...")
            restart_cluster(master_list, worker_list, port, local_disks, image_tag, worker_cpu_limit)
        else:
            print("Unknown operation! " + operation)
    except Exception as ex:
        print(ex)

if __name__ == "__main__":
    main(sys.argv)