# coding=utf-8
# 使用说明：
# Master的watcher脚本，用过类crontab逻辑来管理，不添加long running逻辑
# 输入的master和worker部署节点列表，需要是salt能识别的id

import sys
import random
import socket
import time
import subprocess, threading

MASTER_ADDRESS_FILE = 'master_address'
SUCCESS_FILE = '_SUCCESS'
MASTER_CHECKER_PATH = '/ess_master_address'
RESTART = 'restart'
CHECK = 'check'
BOOTSTRAP = 'bootstrap'


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

def restart_cluster(master_list, workers, port):
    print("begin to restart whole cluster, master_list is " + ",".join(master_list))
    worker_list=workers.split(",")
    diff = list(set(worker_list) - set(master_list))
    random.shuffle(diff)
    candicates=master_list + diff

    try:
        execute_command_with_timeout("hdfs dfs -rm {}/{}".format(MASTER_CHECKER_PATH, SUCCESS_FILE))
        for master_node in candicates:
            if master_node == "":
                continue
            master_node = master_node.split(":")[0]
            print("try start master on " + master_node)
            execute_command_with_timeout(
                'sh restart_cluster.sh {} {} {} {}'.format(master_node, port, workers, ','.join(candicates)), 300)
            if start_check("{}:{}".format(master_node, port)):
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

def start_check(address):
    s = socket.socket()
    host = address.split(":")[0]
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
    from optparse import OptionParser
    parser = OptionParser()

    print("example:{}".format("python master_watcher.py -o boostrap/check -m 192.168.6.85 -p 9099 -f filename"))
    parser.add_option("-o", "--operation", dest="operation", default='check', help="OPERATION for this call",
                      metavar="OPERATION")
    parser.add_option("-m", "--masterList", dest="masterList", help="node list to deploy master", metavar="masterlist")
    parser.add_option("-p", "--port", dest="port", type="int", default=80, help="PORT for server", metavar="PORT")
    parser.add_option("-f", "--workerListFile", dest="workerListFile", help="worker nodes file",
                      metavar="workerListFile")

    (options, args) = parser.parse_args()
    print('options %s ,args %s' % (options, args))
    operation = options.operation
    if options.masterList is None:
        master_list = []
    else:
        master_list = options.masterList.split(",")
    port = options.port
    worker_list = read_worker_list_file(options.workerListFile)

    print("begin to run master watcher, operation " + operation)
    try:
        if operation == BOOTSTRAP:
            execute_command_with_timeout("hdfs dfs -mkdir {}".format(MASTER_CHECKER_PATH))
            address_in_hdfs = get_master_addres_from_hdfs()
            if address_in_hdfs != "":
                master_list.append(address_in_hdfs)
            restart_cluster(master_list, worker_list, port)
        elif operation == CHECK:
            addresses = set(master_list)
            address_in_hdfs = get_master_addres_from_hdfs()
            if address_in_hdfs != "":
                addresses.add(address_in_hdfs)

            for addr in addresses:
                if start_check(addr):
                    print("master alive on " + addr)
                    return
                else:
                    print("master NOT alive on " + addr + ", try next")

            print("no alive master! restart cluster...")
            restart_cluster(master_list, worker_list, port)
        else:
            print("Unknown operation! " + operation)
    except Exception as ex:
        print(ex)

if __name__ == "__main__":
    main(sys.argv)