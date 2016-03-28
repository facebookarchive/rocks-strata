#!/bin/bash

# This assumes that start-stop-daemon executes from /sbin/start-stop-daemon
# You may need to use a different tool if start-stop-daemon is not available for your Linux distribution.

# Example crontab entry:
# 0 */2 * * * /path/to/backup/run.sh BUCKET BUCKET_PREFIX REPLICA_ID 720h
# Every two hours, this will backup, delete backups older than 30 days, and garbage collect

BUCKET=$1
BUCKET_PREFIX=$2
REPLICA_ID=$3
DELETE_OLDER_THAN=$4

RSTRATA_PATH=/path/to/main  # TODO: Replace with your path
LOGFILE="rocks-strata.log"  # TODO: Replace with your path
source example_aws_credentials  # TODO: Replace with your path

# Uses start-stop-daemon because, for a given replica ID, only one write-capable operation should run at once.
/sbin/start-stop-daemon --start --exec $RSTRATA_PATH -- -b=$BUCKET -p=$BUCKET_PREFIX backup -r=$REPLICA_ID >> $LOGFILE 2>&1
/sbin/start-stop-daemon --start --exec $RSTRATA_PATH -- -b=$BUCKET -p=$BUCKET_PREFIX delete -r=$REPLICA_ID -a=$DELETE_OLDER_THAN >> $LOGFILE 2>&1
/sbin/start-stop-daemon --start --exec $RSTRATA_PATH -- -b=$BUCKET -p=$BUCKET_PREFIX gc -r=$REPLICA_ID >> $LOGFILE 2>&1
