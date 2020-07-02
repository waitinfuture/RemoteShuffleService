#!/usr/bin/env bash

./sbin/stop-worker.sh 1
./sbin/stop-worker.sh 2
./sbin/stop-worker.sh 3
./sbin/stop-master.sh

./sbin/start-master.sh

sleep 5

./sbin/start-worker.sh 1
./sbin/start-worker.sh 2
./sbin/start-worker.sh 3




