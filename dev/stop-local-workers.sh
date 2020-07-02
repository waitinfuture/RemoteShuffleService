#!/usr/bin/env bash

WORKER_INSTANCE=1 ./sbin/stop-worker.sh
WORKER_INSTANCE=2 ./sbin/stop-worker.sh
WORKER_INSTANCE=3 ./sbin/stop-worker.sh
./sbin/stop-master.sh




