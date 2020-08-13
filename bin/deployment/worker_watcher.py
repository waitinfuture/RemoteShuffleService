# coding=utf-8
# 注意事项：
# 默认5min执行一次检查，如果刚好触发检查的时候再重启呢？目前是重试一定次数来获取
# 这个脚本不支持rss本身的worker混部

import time
import subprocess, threading

MASTER_ADDRESS_FILE = 'master_address'
MASTER_CHECKER_PATH = '/ess_master_address'
SUCCESS_FILE = '_SUCCESS'


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


def get_master_address():
    print("begin to get master address from hdfs")
    for _ in range(3):
        try:
            _, master_address_files, _ = execute_command_with_timeout("hdfs dfs -ls {}".format(MASTER_CHECKER_PATH) +
                                                                      "| awk '{print $8}' | awk -F'/' '{print $NF}'")
            has_success = False if SUCCESS_FILE not in master_address_files else True
            print("success file status {}".format(has_success))
            if has_success:
                _, address, _ = execute_command_with_timeout("hdfs dfs -cat {}/{}".format(
                    MASTER_CHECKER_PATH, MASTER_ADDRESS_FILE))
                print("current master address {}".format(address))
                return address
        except Exception as ex:
            print(ex)
        time.sleep(5)
    return ""


def check_worker_process():
    for _ in range(8):
        try:
            _, worker_num, _ = execute_command_with_timeout('ps aux | grep Worker | grep ess | wc -l')
            # use gevent worker_num return value is begin from 1
            if int(worker_num) > 1:
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
