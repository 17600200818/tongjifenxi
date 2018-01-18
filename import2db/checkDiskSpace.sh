#!/bin/sh

ip=`/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
hname=`hostname`

result=`/bin/df -k | awk '{if(int($5) > 0){print int($5)}}'`

for rate in $result
do
    if [ $rate -gt 75 ]; then
#        echo "rate : $rate"
        /usr/local/bin/sendEmail -f sendmail@rtbs.cn -t zhangzhijian@rtbs.cn -s smtp.exmail.qq.com -xu sendmail@rtbs.cn -xp pX1tKE9v  -u "$hname disk space $rate%" -m "from : $ip"  -o message-charset=utf-8 -o message-content-type=html
    fi
done
