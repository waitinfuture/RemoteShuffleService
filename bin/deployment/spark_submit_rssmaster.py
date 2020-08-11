# coding=utf-8
# 注意事项：
# 上层脚本如果看到输出为空，就fall back
# 不存在success或者是检测失败就返回空，方便上层提交脚本判断

import socket
import time
import gevent
from gevent import subprocess, Timeout

MASTER_ADDRESS_FILE = 'master_address'
SUCCESS_FILE = '_SUCCESS'
MASTER_CHECKER_PATH = '/ess_master_address'


def execute_command_with_timeout(command, timeout=300):
    t = Timeout(timeout)
    try:
        t.start()
        gevent.sleep(0.01)
        print("command: {}".format(command))
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        output_str = list()
        while True:
            output = p.stdout.readline()
            if output == '' and p.poll() is not None:
                break
            if output:
                print(output.strip())
                output_str.append(output.strip())
        rc = p.poll()
        return rc, ''.join(output_str), ''
    except Timeout as e:
        try:
            p.terminate()
        except OSError as e:
            print("process terminate failed.")
        return 0x7f, '', 'Timeout'
    finally:
        t.cancel()


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


if __name__ == "__main__":
    print(get_master_address())
