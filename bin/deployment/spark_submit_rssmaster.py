# coding=utf-8
# 注意事项：
# 上层脚本如果看到输出为空，就fall back
# 不存在success或者是检测失败就返回空，方便上层提交脚本判断

import subprocess
import socket
import time

MASTER_ADDRESS_FILE = 'master_address'
SUCCESS_FILE = '_SUCCESS'
MASTER_CHECKER_PATH = '/ess_master_address'


def execute_command_with_timeout(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    try:
        p.wait(5)
        return p
    except subprocess.TimeoutExpired as e:
        p.kill()
        raise e


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
        master_address_files = bytes.decode(
            execute_command_with_timeout("hdfs dfs -ls {}".format(MASTER_CHECKER_PATH) +
                                         "| awk '{print $8}' | awk -F'/' '{print $NF}'").stdout.read())
        has_success = False if SUCCESS_FILE not in master_address_files else True
        print("success file status {}".format(has_success))
        if has_success:
            address = bytes.decode(execute_command_with_timeout(
                "hdfs dfs -cat {}/{}".format(MASTER_CHECKER_PATH, MASTER_ADDRESS_FILE)).stdout.read())
            print("current master address {}".format(address))
            if start_check(address):
                return address
    except Exception as ex:
        print(ex)
    return ""


if __name__ == "__main__":
    print(get_master_address())
