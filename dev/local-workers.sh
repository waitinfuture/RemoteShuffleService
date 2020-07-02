#!/usr/bin/env bash

./dev/stop-local-workers.sh

./sbin/start-master.sh

sleep 5

WORKER_INSTANCE=1 ./sbin/start-worker.sh
WORKER_INSTANCE=2 ./sbin/start-worker.sh
WORKER_INSTANCE=3 ./sbin/start-worker.sh




