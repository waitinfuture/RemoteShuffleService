# coding=utf-8
# 注意事项：
# 默认5min执行一次检查，如果刚好触发检查的时候再重启呢？目前是重试一定次数来获取
# 这个脚本不支持rss本身的worker混部

import traceback
import os
import subprocess
import time

MASTER_ADDRESS_FILE = 'master_address'
MASTER_CHECKER_PATH = '/ess_master_address'
SUCCESS_FILE = '_SUCCESS'


def get_master_address():
    print("begin to get master address from hdfs")
    for _ in range(3):
        try:
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
                return address
        except Exception, ex:
            print  traceback.format_exc()
        time.sleep(5)
    return ""


def check_worker_process():
    for _ in range(8):
        try:
            worker_num = os.popen('ps aux | grep Worker | grep ess | wc -l').read()
            if worker_num > 0:
                return True
        except Exception, ex:
            print  traceback.format_exc()
        time.sleep(6)
    return False


def check_worker_status():
    try:
        if not check_worker_process():
            master_address=get_master_address()
            if master_address == "":
                print "master address can not be fetched, so we will not restart this worker"
                return
            os.system('sh restart_worker.sh ess://{}'.format(master_address))

    except Exception, ex:
        print  traceback.format_exc()


if __name__ == "__main__":
    check_worker_status()
