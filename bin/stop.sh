#!/bin/bash

ps -aAFf |  grep [a]irflow | awk '{print $2}' | awk '{kill -9 $0}'
ps -aAFf |  grep [a]irflow | awk '{print $2}' | awk '{kill -9 $0}' | xargs kill -9
kill -9 `ps aux | grep airflow | awk '{print $2}'`