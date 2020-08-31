# coding=utf-8
# 注意事项：
# 上层脚本如果看到输出为空，就fall back
# 不存在success或者是检测失败就返回空，方便上层提交脚本判断

import os
import socket
import sys
import time
import subprocess, threading

MASTER_ADDRESS_FILE = 'master_address'
SUCCESS_FILE = '_SUCCESS'
MASTER_CHECKER_PATH = '/ess_master_address'


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


def get_master_address():
    print("begin to get master address from hdfs")
    try:
        _, master_address_files, _ = execute_command_with_timeout("hdfs dfs -ls {}".format(MASTER_CHECKER_PATH) +
                                                                  "| awk '{print $8}' | awk -F'/' '{print $NF}'")
        has_success = False if SUCCESS_FILE not in master_address_files else True
        print("success file status {}".format(has_success))
        if has_success:
            _, address, _ = execute_command_with_timeout("hdfs dfs -cat {}/{}".format(
                MASTER_CHECKER_PATH, MASTER_ADDRESS_FILE))
            print("current master address {}".format(address))
            if start_check(address):
                return address
    except Exception as ex:
        print(ex)
    return ""


def main(argv):
    master_address=get_master_address()
    if master_address == '':
        print("run spark without ESS")
        os.system('spark-sql ' + ' '.join(argv[1:]))
    else:
        print("run spark with ESS")
        os.system('spark-sql --conf spark.shuffle.manager=org.apache.spark.shuffle.ess.EssShuffleManager --conf spark.ess.master.address=' + master_address + ' ' + ' '.join(argv[1:]))

if __name__ == "__main__":
    main(sys.argv)
