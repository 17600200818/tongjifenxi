<?php
/**
 * create date : 2013-5-29
 * description :
 *
 * @author:	zhangzhijian
 * @copyright:
 */

define("TOP100",	100);
define("TOP1000",	1000);


/**
 *  函数:
 *  描述: 检查服务是否存在
 *  输入:
 *  输出:
 **/
function checkServiceExists()
{
    global $arrCfg, $pidFile, $serviceName;

    if (file_exists($pidFile)){

        $pidFileCtx = file_get_contents($pidFile);

        $arrPid = explode("|", $pidFileCtx);

        if (empty($arrPid) || count($arrPid) != 3){
            return false;
        }

        $startRunTime = intval($arrPid[0]);
        $lastRunPid	  = trim($arrPid[1]);
        $lastAlarmTm  = intval($arrPid[2]);

        $rtn = exec('ps '.$lastRunPid);
        if(strpos($rtn, $lastRunPid) === FALSE){

            //	如果进程已经不存在，就删除pid文件
            if (file_exists($pidFile)){
                unlink($pidFile);
            }

            // send alarm mail.
            $ctx = sprintf("process %ld no exists. startRunTime:%s.", $lastRunPid, date("Y-m-d H:i:s", $startRunTime));
            sendEmail($arrCfg['service']['recvEmail'], $serviceName." service no exists", $ctx);
        }
        else{
            $timeoutLimit = intval($arrCfg['service']['timeout']);

            if ($lastAlarmTm == 0){

                //	如果没有发送过报警邮件，当前时间与开始运行时间对比，超出时间限制即发送邮件报警
                $timeout = time() - $startRunTime;
                if ($timeout > $timeoutLimit){
                    file_put_contents($pidFile, $startRunTime."|".$lastRunPid."|".time());

                    // send alarm mail.
                    $ctx = sprintf("%s first run timeout. startRunTime:%s, pid:%ld, timeout:%lds.",
                        $serviceName, date("Y-m-d H:i:s", $startRunTime), $lastRunPid, $timeout);
                    sendEmail($arrCfg['service']['recvEmail'], $serviceName.' run timeout', $ctx);
                }
                else{
                    print_r(sprintf("[%s] pid: %d, start run time: %s ...\n", date("H:i:s"), $lastRunPid, date("Y-m-d H:i:s", $startRunTime)));
                }
            }
            else{
                //	如果有发送过报警邮件，当前时间与上次报警时间对比，超出时间限制即发送邮件报警
                $timeout = time() - $lastAlarmTm;
                if ($timeout > $timeoutLimit){

                    file_put_contents($pidFile, $startRunTime."|".$lastRunPid."|".time());

                    $timeout = time() - $startRunTime;

                    // send alarm mail.
                    $ctx = sprintf("%s run timeout N-th. startRunTime:%s, pid:%ld, timeout:%lds.",
                        $serviceName, date("Y-m-d H:i:s", $startRunTime), $lastRunPid, $timeout);
                    sendEmail($arrCfg['service']['recvEmail'], $serviceName.' run timeout', $ctx);
                }
                else{
                    print_r(sprintf("[%s] pid: %d, start run time: %s ...\n", date("H:i:s"), $lastRunPid, date("Y-m-d H:i:s", $startRunTime)));
                }
            }
        }

        die();
    }
    else{
        file_put_contents($pidFile, time()."|".getmypid()."|0");
    }

    return true;
}

/**
 *  函数:
 *  描述: 发送提醒邮件
 *  输入:
 *  输出:
 **/
function sendEmail($recvEmail, $title, $mailCtx)
{
    global $arrCfg, $serviceFile, $argv;

    if (empty($recvEmail)){
        return false;
    }

    $fileSign = sprintf("\nat:\t%s\nhost:\t%s\npath:\t%s\n\n", date("Y-m-d H:i:s"), system('hostname'), $serviceFile);
    foreach ($argv as $key=>$val) {
        $fileSign .= sprintf("%s\t", $val);
    }
//    $fileSign = encrypt($fileSign."\n");

    $alarmCmd  = sprintf(" %s -f sendmail@rtbs.cn ", trim($arrCfg['service']['mailBin']));
    $alarmCmd .= sprintf(" -t %s ", trim($recvEmail));
    $alarmCmd .= sprintf(" -s smtp.exmail.qq.com -xu %s -xp %s ", trim($arrCfg['service']['sendEmail']), trim($arrCfg['service']['pwd']));
    $alarmCmd .= sprintf(" -u 'rtbs stat: %s' -m '%s\r\n\r\n\r\nfrom:\t%s\nfileSign:\t%s'", $title, $mailCtx, system('hostname'), $fileSign);

    system($alarmCmd);

    return true;
}

/**
 *  函数: 对文本数据进行加密
 *  描述:
 *  输入:
 *  输出:
 **/
function encrypt($ctx)
{
    $ctx = base64_encode($ctx);

    $ctx = trim($ctx, "=");

    $ctx = strrev($ctx);

    return $ctx;
}

/**
 *  函数: 得到url的域名
 *  描述:
 *  输入:
 *  输出:
 **/
function getDomain($url)
{
    static $arrNeedle = array('%');
    static $arrFilter = array(' ', '\'', '\"');

    $url = strtolower(trim(urldecode($url)));
    if (empty($url) || $url == 'null'){
        return -1;
    }

    $url = trim(urldecode($url));

    if (strlen($url)<3){
        return -1;
    }

    if ( substr($url, 0, 7) != 'http://' ) {
        $url = 'http://'.$url;
    }

    $rtn = @parse_url($url);
    if (is_array($rtn) && isset($rtn['host'])){

        $domain = $rtn['host'];

        reset($arrNeedle);
        foreach ($arrNeedle as $needle) {
            $pos = strpos($domain, $needle, 0);
            if ($pos !== false){
                $domain = substr($domain, 0, $pos);
            }
        }

        if (strstr($domain, '.') === false){
            return -1;
        }

        reset($arrFilter);
        foreach ($arrFilter as $filter) {
            if (strstr($domain, ' ')){
                return -1;
            }
        }

        $domain = trim($domain);

        if (preg_match("/^[a-z0-9.-]+$/", $domain) == 0){
            return -1;
        }

        if (strlen($domain) > 50 || strlen($domain) < 3){
            return -1;
        }

        $preg="/\A((([0-9]?[0-9])|(1[0-9]{2})|(2[0-4][0-9])|(25[0-5]))\.){3}(([0-9]?[0-9])|(1[0-9]{2})|(2[0-4][0-9])|(25[0-5]))\Z/";
        if(preg_match($preg, $domain)){
            return -1;
        }

        return $domain;
    }

    return -1;
}

/**
 * 得到主域
 */
function getPrimaryDomain($url)
{
    $domain = 'com|cn|mobi|co|net|com.cn|net.cn|so|org|gov|gov.cn|org.cn|tel|tv|biz|cc|hk|name|info|asia|me|edu|tv|la|ad|ae|af|' .
        'ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cf|cg|ch|ci|ck|cl|' .
        'cm|cn|co|cq|cr|cu|cv|cx|cy|cz|de|dj|dk|dm|do|dz|ec|ee|eg|eh|es|et|ev|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gh|gi|gl|gm|gn|gp|' .
        'gr|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|in|io|iq|ir|is|it|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|' .
        'ls|lt|lu|lv|ly|ma|mc|md|mg|mh|ml|mm|mn|mo|mp|mq|mr|ms|mt|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nt|nu|nz|om|qa|pa|' .
        'pe|pf|pg|ph|pk|pl|pm|pn|pr|pt|pw|py|re|ro|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sk|sl|sm|sn|so|sr|st|su|sy|sz|tc|td|tf|tg|th|' .
        'tj|tk|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|va|vc|ve|vg|vn|vu|wf|ws|ye|yu|za|zm|zr|zw|im|cd|uz|eu|aero';

    $domainName = -1;

    if ( substr($url, 0, 7) != 'http://' ) {
        $url = 'http://'.$url.'/';//类似 www.shuangtv.net 的url,开头加上 http://
    }

    if ( substr($url, -1, 1) != '/' ) {
        $url = $url.'/';//类似 http://www.shuangtv.net 的url,结尾加 /
    }

    $url = trim(urldecode(strtolower($url)), "%.%2.%2get");
    $url = str_replace(array('%2'),'',$url);
    $url = str_replace(array('%:f','%'),'/',$url);
    $url = str_replace(array(':///','://://'),'://',$url);

    if(strpos($url,'|') || strpos($url,'\'') || strpos($url,'\\')){
        return -1;
    }

    if ($url != null && $url != 'null' && $url != 'http://'){
        $parseAry = @parse_url($url);
        if (isset($parseAry['host'])){
            $host = strtolower($parseAry['host']);
            $hostTmp = $host;
            $preg="/\A((([0-9]?[0-9])|(1[0-9]{2})|(2[0-4][0-9])|(25[0-5]))\.){3}(([0-9]?[0-9])|(1[0-9]{2})|(2[0-4][0-9])|(25[0-5]))\Z/";
            if(preg_match($preg,$host)){
                return -1;
            }else{
                $hostTmp = preg_replace('/(\.('.$domain.'))*\.('.$domain.')$/iU','',$hostTmp);//去掉顶级域
                $domainAry = explode('.', $hostTmp);
                $domainName = array_pop($domainAry);
                $domainName = substr($host, strrpos($host, $domainName));
            }
        }else if (isset($parseAry['path'])){
            $pathTmp = $parseAry['path'];
            $pathTmp = preg_replace('/(\.('.$domain.'))*\.('.$domain.')$/iU','',$pathTmp);//去掉顶级域
            $domainAry = explode('.', $pathTmp);
            $domainName = array_pop($domainAry);
            $domainName = substr($parseAry['path'], strrpos($parseAry['path'], $domainName));
        }
    }

    if (is_string($domainName)){
        $domainName = trim($domainName);

        if (!strpos($domainName, '.') || strlen($domainName) > 50 || strlen($domainName) < 3){	//不是正常url
            return -1;
        }
    }

    return $domainName;
}

$arrErrFile = array();

/**
 *  函数:
 *  描述: 记录错误日志
 *  输入:
 *  输出:
 **/
function writeErrorLogs($errcode, $time, $ctx=null)
{
    global $arrErrFile, $errPath, $filename, $line;

    $key = sprintf("%d_%s", $errcode, $filename);

    if (!isset($arrErrFile[$key])){

        $errDir = sprintf("%s/%s/%d", $errPath, date("Ym/d", $time), $errcode);
        if (!is_dir($errDir)){
            system("mkdir -p ".$errDir);
            system("chmod -R 755 ".$errDir);
        }

        $errFile = sprintf("%s/%s.gz", $errDir, $filename);
        $hLog = gzopen($errFile, 'a');
        if (!$hLog)	{
            return false;
        }

        $arrErrFile[$key] = $hLog;
    }

    if (is_null($ctx)){
        gzwrite($arrErrFile[$key], $line."\n");
    }
    else{
        gzwrite($arrErrFile[$key], $ctx."\n");
    }

    return true;
}

/**
 *  描述: 得到url中指定参数的值
 *  输入:
 *  输出:
 **/
function getUrlParam($params, $name)
{
    $arrParam = explode("&", $params);

    if (!empty($arrParam)){
        foreach ($arrParam as $param) {
            $arrTemp = explode("=", $param);
            if (!empty($arrTemp)){
                $key = trim($arrTemp[0]);
                if ($key == $name){
                    return trim($arrTemp[1]);
                }
            }
        }
    }

    return null;
}

?>