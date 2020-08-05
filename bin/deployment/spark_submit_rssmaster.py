# coding=utf-8
# 注意事项：
# 上层脚本如果看到输出为空，就fall back
# 不存在success或者是检测失败就返回空，方便上层提交脚本判断

import subprocess
import traceback

MASTER_ADDRESS_FILE = 'master_address'
SUCCESS_FILE = '_SUCCESS'
MASTER_CHECKER_PATH = '/ess_master_address'


def get_master_address():
    print("begin to get master address from hdfs")
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
    return ""


if __name__ == "__main__":
    print get_master_address()
