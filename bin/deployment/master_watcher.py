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


# return master host
def select_master_host(master_list):
    random.shuffle(master_list)
    print("master list:")
    print(master_list)
    for host in master_list:
        return_code, _, _ = execute_command_with_timeout("ping -c 1 " + host)
        if return_code == 0:
            return host
    return ""


def restart_cluster(master_list, worker_list, old_master, port):
    print("begin to restart whole cluster")
    old_master_host = ""
    if old_master != "":
        old_master_host = old_master.split(":")[0]
    try:
        execute_command_with_timeout("hdfs dfs -rm {}/{}".format(MASTER_CHECKER_PATH, SUCCESS_FILE))
        master_node = select_master_host(master_list)
        if master_node == "":
            raise ValueError('there is no alive master node to start master service!')
        execute_command_with_timeout(
            'sh restart_cluster.sh {} {} {} {}'.format(master_node, port, worker_list, old_master_host), 300)

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
        else:
            print("master process is not ready, please check deployment status!")
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


def main(argv):
    from optparse import OptionParser
    parser = OptionParser()

    print("example:{}".format("python master_watcher.py -o check -m 192.168.6.85 -p 9099 -f filename"))
    parser.add_option("-o", "--operation", dest="operation", default='check', help="OPERATION for this call",
                      metavar="OPERATION")
    parser.add_option("-m", "--masterList", dest="masterList", help="node list to deploy master", metavar="masterlist")
    parser.add_option("-p", "--port", dest="port", type="int", default=80, help="PORT for server", metavar="PORT")
    parser.add_option("-f", "--workerListFile", dest="workerListFile", help="worker nodes file",
                      metavar="workerListFile")

    (options, args) = parser.parse_args()
    print('options %s ,args %s' % (options, args))
    operation = options.operation
    master_list = options.masterList.split(",")
    port = options.port
    worker_list = read_worker_list_file(options.workerListFile)

    print("begin to run master watcher")
    try:
        if operation == BOOTSTRAP:
            execute_command_with_timeout("hdfs dfs -mkdir {}".format(MASTER_CHECKER_PATH))
            restart_cluster(master_list, worker_list, "", port)
        else:
            _, master_address_files, _ = execute_command_with_timeout("hdfs dfs -ls {}".format(MASTER_CHECKER_PATH) +
                                                                      "| awk '{print $8}' | awk -F'/' '{print $NF}'")
            has_success = False if SUCCESS_FILE not in master_address_files else True
            print("success file status {}".format(has_success))
            if has_success:
                _, address, _ = execute_command_with_timeout("hdfs dfs -cat {}/{}".format(
                    MASTER_CHECKER_PATH, MASTER_ADDRESS_FILE))
                print("current master address {}".format(address))
                if address == "":
                    raise ValueError('address can not be empty!')

                if operation == CHECK:
                    master_alive = start_check(address)
                    if not master_alive:
                        restart_cluster(master_list, worker_list, address, port)
                else:
                    restart_cluster((address.split(":")[0]).split(), worker_list, address, port)
            else:
                print("please bootstrap first since there is no master address on hdfs!")
    except Exception as ex:
        print(ex)


if __name__ == "__main__":
    main(sys.argv)
