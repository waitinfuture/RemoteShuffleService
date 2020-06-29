#!/usr/bin/env bash

ps aux | grep "service.deploy.master.Master" | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | grep "service.deploy.worker.Worker" | grep -v grep | awk '{print $2}' | xargs kill -9




