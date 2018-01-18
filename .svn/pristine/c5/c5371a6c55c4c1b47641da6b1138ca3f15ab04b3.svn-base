#!/bin/sh

ROOT=/home/rtb/statV2/updateReportData

cd $ROOT

LOG_MONTH=$(date +%Y%m)/$(date +%d)
LOG_DATE=$(date +%Y%m%d)

LOG_DIR=$ROOT/../tmp/updateReportData/log/$LOG_MONTH
mkdir -p $LOG_DIR

LOGFILE=$LOG_DIR/$LOG_DATE.stat.log
/usr/local/bin/php updateReportData.php  >> $LOGFILE &

