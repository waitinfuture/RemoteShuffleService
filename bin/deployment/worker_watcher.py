# coding=utf-8
# 注意事项：
# 默认5min执行一次检查，如果刚好触发检查的时候再重启呢？目前是重试一定次数来获取
# 这个脚本不支持rss本身的worker混部

import subprocess
import time

MASTER_ADDRESS_FILE = 'master_address'
MASTER_CHECKER_PATH = '/ess_master_address'
SUCCESS_FILE = '_SUCCESS'


def execute_command_with_timeout(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    try:
        p.wait(5)
        return p
    except subprocess.TimeoutExpired as e:
        p.kill()
        raise e


def get_master_address():
    print("begin to get master address from hdfs")
    for _ in range(3):
        try:
            master_address_files = bytes.decode(
                execute_command_with_timeout("hdfs dfs -ls {}".format(MASTER_CHECKER_PATH) +
                                             "| awk '{print $8}' | awk -F'/' '{print $NF}'").stdout.read())
            has_success = False if SUCCESS_FILE not in master_address_files else True
            print("success file status {}".format(has_success))
            if has_success:
                address = bytes.decode(
                    execute_command_with_timeout("hdfs dfs -cat {}/{}".format(MASTER_CHECKER_PATH,
                                                                              MASTER_ADDRESS_FILE)).stdout.read())
                print("current master address {}".format(address))
                return address
        except Exception as ex:
            print(ex)
        time.sleep(5)
    return ""


def check_worker_process():
    for _ in range(8):
        try:
            worker_num = int(bytes.decode(
                execute_command_with_timeout('ps aux | grep Worker | grep ess | wc -l').stdout.read()))
            if worker_num > 0:
                return True
        except Exception as ex:
            print(ex)
        time.sleep(6)
    return False


def check_worker_status():
    try:
        if not check_worker_process():
            master_address = get_master_address()
            if master_address == "":
                print("master address can not be fetched, so we will not restart this worker")
                return
            execute_command_with_timeout('sh restart_worker.sh ess://{}'.format(master_address))
        else:
            print("worker process is alive!")

    except Exception as ex:
        print(ex)


if __name__ == "__main__":
    check_worker_status()
