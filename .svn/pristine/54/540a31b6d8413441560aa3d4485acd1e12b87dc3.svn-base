#!/bin/sh

ROOT=/home/rtb/statV2Trunk/import2db

cd $ROOT

currHour=$(date +%H)
currMinute=$(date +%M)
if [ $currMinute -eq 30 ]
then
    ./checkDiskSpace.sh  >> /dev/null 2>&1
    if [ $currHour -eq 3 ]
    then
        /usr/local/bin/php createTables.php  conf/import2db.ini >> $LOG_DIR/createdb.$LOG_DATE.log
        /usr/local/bin/php exportDatabases.php  conf/import2db.ini >> $LOG_DIR/createdb.$LOG_DATE.log
    fi
fi

LOG_MONTH=$(date +%Y%m)/$(date +%d)
LOG_DATE=$(date +%Y%m%d)

LOG_DIR=$ROOT/../tmp/import2db/log/$LOG_MONTH
mkdir -p $LOG_DIR

/usr/local/bin/php import2db.php  conf/import2db.05min.ini >> $LOG_DIR/$LOG_DATE.import2db.05min.log
/usr/local/bin/php import2db.php  conf/import2db.20min.ini >> $LOG_DIR/$LOG_DATE.import2db.20min.log
/usr/local/bin/php import2db.php  conf/import2db.30min.ini >> $LOG_DIR/$LOG_DATE.import2db.30min.log
/usr/local/bin/php import2db.php  conf/import2db.60min.ini >> $LOG_DIR/$LOG_DATE.import2db.60min.log


