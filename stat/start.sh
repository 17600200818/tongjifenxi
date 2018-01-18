#!/bin/sh

ROOT=/home/rtb/statV2Trunk/stat

cd $ROOT

LOG_MONTH=$(date +%Y%m)/$(date +%d)
LOG_DATE=$(date +%Y%m%d)

LOG_DIR=$ROOT/../tmp/stat/log/$LOG_MONTH
mkdir -p $LOG_DIR

LOGFILE=$LOG_DIR/$LOG_DATE.stat.log
/usr/local/bin/php stat.php  >> $LOGFILE &
/usr/local/bin/php backup.php  >> /dev/null 2>&1

