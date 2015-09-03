#!/bin/bash

# Setup:
# Make sure that you can send email with
# `echo "test body" | mail -s "test subject" you@you.com`
# If you don't receive the test email, consider `sudo apt-get install mailutils`
# You will also need a rocks-strata executable on the correct path, etc. 

# Example usage:
# Suppose that you have scheduled backups of REPLICA_ID every two hours.
# Consider a crontab entry:
# 0 * * * * bash /path/to/monitor.sh BUCKET BUCKET_PREFIX REPLICA_ID 14400 you@you.com
# This will run monitor.sh every hour and alert you@you.com if the most recent
# backup for REPLICA_ID is more than four hours (14400 seconds) old.

BUCKET=$1
BUCKET_PREFIX=$2
REPLICA_ID=$3
TOO_LONG_SECONDS=$4
ALERT_ADDRESS=$5

source example_aws_credentials

msg_subject="rocks-strata alert for $REPLICA_ID"

last=$(./main -b=$BUCKET -p=$BUCKET_PREFIX show last-backup-time -r=$REPLICA_ID)
now=$(date +%s)

if [ -z $last ] 
then
    msg_body="no backup found for $REPLICA_ID"
    echo $msg_body
    echo $msg_body | mail -s $msg_subject $ALERT_ADDRESS
    exit
fi

elapsed=$(($now - $last))
if [ "$elapsed" -ge "$TOO_LONG_SECONDS" ] 
then
    last_human=$(date -d @$last)
    msg_body="last backup of $REPLICA_ID was on $last_human"
    echo $msg_body
    echo $msg_body | mail -s $msg_subject $ALERT_ADDRESS
fi
