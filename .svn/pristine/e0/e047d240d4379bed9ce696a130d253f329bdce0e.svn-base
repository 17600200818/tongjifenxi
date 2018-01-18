<?php
ini_set('error_reporting',		'E_ALL');
ini_set('max_execution_time',	'0');
ini_set("memory_limit",			"-1");
ini_set('display_errors',		'on');
ini_set('log_errors',			'off');

//根目录
define('BASE_PATH', dirname(__FILE__));
date_default_timezone_set('PRC');
set_include_path(implode(PATH_SEPARATOR, array(BASE_PATH, BASE_PATH."/../lib/", get_include_path())));

require_once("db.php");
require_once("comm.php");
require_once("redis.lib.php");

$startRunTime = time();

$serviceFile = __FILE__;
$pathParts	 = pathinfo($serviceFile);
$serviceName = str_ireplace('.php', '', $pathParts['basename']);

if($argc == 1) {
    $cfgFile = sprintf("%s/conf/%s.ini", BASE_PATH, $serviceName);
}
else{
    $cfgFile = sprintf("%s", $argv[1]);
}

if (!file_exists($cfgFile)){
    die("ini file ".$cfgFile." no exists.\n");
}

$arrCfg = parse_ini_file($cfgFile, true);

$pidFile = sprintf("%s.pid", $cfgFile);

checkServiceExists();

$logPath	= sprintf("%s/../%s", BASE_PATH, $arrCfg['import2db']['logpath']);
$outPath	= sprintf("%s/../%s", BASE_PATH, $arrCfg['import2db']['outpath']);
$tmpPath	= sprintf("%s/../%s", BASE_PATH, $arrCfg['import2db']['tmppath']);
$errPath	= sprintf("%s/../%s", BASE_PATH, $arrCfg['import2db']['errpath']);

if (!is_dir($logPath)){
    system("mkdir -p ".$logPath);
    system("chmod -R 755 ".$logPath);
}

$readFileNum = intval($arrCfg['import2db']['readFileNum']);

//  load redis param
$redisList = explode(',', $arrCfg['import2db']['redis']);

$cfgSrcFileFlag = trim($arrCfg['import2db']['srcFileFlag']);
if (empty($cfgSrcFileFlag)){
    $arrSrcFileFlag = null;
}
else{
    $arrSrcFileFlag = explode(",", $cfgSrcFileFlag);
}

$arrResult = $arrFinishFiles = array();

$arrLogFiles = scandir($logPath);

print_r(sprintf("[%s] scandir: %s\t[%d]\n", date("H:i:s"), $logPath, count($arrLogFiles)-2));
if (!empty($arrLogFiles) && count($arrLogFiles) > 2) {
    $processFileNum = 0;
    sort($arrLogFiles);
    loadParam();
    foreach ($arrLogFiles as $filename) {
        if ($filename == '.' || $filename == '..') {
            continue;
        }

        if ($processFileNum >= $readFileNum) {
            break;
        }

        $logFile = sprintf("%s/%s", $logPath, $filename);
        $pathParts = pathinfo($filename);
        $temp1 = explode("=", $pathParts['basename']);
        if (count($temp1) != 3) {
            $cmd = sprintf("mv %s %s", $filename, $errPath);
            system($cmd);
            continue;
        }
        $host = trim($temp1[0]);
        $temp2 = explode(".", $temp1[2]);
        if (count($temp2) != 4) {
            $cmd = sprintf("mv %s %s", $filename, $errPath);
            system($cmd);
            continue;
        }
        $dbName = $temp2[0];
        $tableName = $temp2[1];
        $date = $temp2[2];

        if (!empty($arrSrcFileFlag)) {
            $findFlag = false;

            reset($arrSrcFileFlag);
            foreach ($arrSrcFileFlag as $srcFileFlag) {
                if ($tableName == $srcFileFlag) {
                    $findFlag = true;
                    break;
                }
            }

            if ($findFlag == false) {
                continue;
            }
        }
        $processFileNum++;

        $filesize = number_format(filesize($logFile) / 1024, 2, '.', '');
        print_r(sprintf("[%s] check [%06d]: %s\t[%01.2fK]\n", date("H:i:s"), $processFileNum, $filename, $filesize));

        $ctx = file_get_contents($logFile);

        $arrNew = json_decode($ctx, true);

        if ($arrNew == false || empty($arrNew)) {
            $cmd = sprintf("mv %s %s", $logFile, $errPath);
            system($cmd);
            continue;
        }
        if (!isset($arrResult[$dbName][$tableName][$date])) {
            loadCache($dbName, $tableName, $date);
        }
//        print_R($arrResult);
        if ($dbName == "adx_report_sell") {
            if ($tableName == "media_day") {
                foreach ($arrNew as $mediaId => $val) {
                    foreach ($val as $k => $v) {
                        $arrResult[$dbName][$tableName][$date][$host][$mediaId][$k] = $v;
                    }
                }
            } else if ($tableName == "media_hour") {
                foreach ($arrNew as $hour => $v1) {
                    foreach ($arrNew as $placeId => $val) {
                        foreach ($val as $k => $v) {
                            $arrResult[$dbName][$tableName][$date][$host][$hour][$placeId][$k] = $v;
                        }
                    }
                }
            } else if ($tableName == "place_day") {
                foreach ($arrNew as $placeId => $val) {
                    foreach ($val as $k => $v) {
                        $arrResult[$dbName][$tableName][$date][$host][$placeId][$k] = $v;
                    }
                }
            }else if ($tableName == "summary_day") {
                foreach ($arrNew as $sellerId => $val) {
                    foreach ($val as $k => $v) {
                        $arrResult[$dbName][$tableName][$date][$host][$sellerId][$k] = $v;
                    }
                }
            }else if ($tableName == "size_day") {
                foreach ($arrNew as $sellerId => $val) {
                    foreach($val as $w => $val1) {
                        foreach ($val1 as $h => $val4) {
                            foreach ($val4 as $k => $v) {
                                $arrResult[$dbName][$tableName][$date][$host][$sellerId][$w][$h][$k] = $v;
                            }
                        }
                    }
                }
            }
        }else if ($dbName == "adx_report_buy") {
            if ($tableName == "summary_day") {
                    foreach ($arrNew as $buyerId => $val2) {
                        foreach ($val2 as $k => $v) {
                            $arrResult[$dbName][$tableName][$date][$host][$buyerId][$k] = $v;
                        }
                    }
            }
        }else if ($dbName == "adx_report") {
            if ($tableName == "summary_day") {
                foreach ($arrNew as $sellerId => $val1) {
                    foreach ($val1 as $buyerId => $val2) {
                        foreach ($val2 as $k => $v) {
                            $arrResult[$dbName][$tableName][$date][$host][$sellerId][$buyerId][$k] = $v;
                        }
                    }
                }
            }else if ($tableName == "place_day") {
                foreach ($arrNew as $placeId => $val1) {
                    foreach ($val1 as $buyerId => $val2) {
                        foreach ($val2 as $k => $v) {
                            $arrResult[$dbName][$tableName][$date][$host][$placeId][$buyerId][$k] = $v;
                        }
                    }
                }
            }else if ($tableName == "place_hour") {
                foreach ($arrNew as $hour => $v1) {
                    foreach ($v1 as $placeId => $v2) {
                        foreach ($v2 as $buyerId => $v3) {
                            $arrResult[$dbName][$tableName][$date][$hour][$placeId][$buyerId]["update"] = true;
                            foreach ($v3 as $k => $v) {
                                $arrResult[$dbName][$tableName][$date][$hour][$placeId][$buyerId][$host][$k] += $v;
                            }
                        }
                    }
                }
            }else if ($tableName == "failure_day") {
                foreach ($arrNew as $buyerId => $val1) {
                    foreach ($val1 as $placeId => $val2) {
                        foreach ($val2 as $crId => $val3) {
                            foreach ($val3 as $errId => $v4) {
                                foreach ($v4 as $k => $v) {
                                    $arrResult[$dbName][$tableName][$date][$host][$buyerId][$placeId][$crId][$errId][$k] = $v;
                                }
                            }
                        }
                    }
                }
            }
        }
//        print_R($arrResult);

        writeCache($dbName, $tableName, $date, $arrResult[$dbName][$tableName][$date]);

        $arrFinishFiles[] = $logFile;
    }

    //print_R($arrResult);

    if (!empty($arrResult)) {
        foreach ($arrResult as $dbName => $v1){
            foreach ($v1 as $tableName => $v2) {
                foreach ($v2 as $date => $v3) {

                    $time = strtotime($date);
                    $sqlDir	= sprintf("%s/%s", $outPath, date("Ym/d", $time));
                    if (!file_exists($sqlDir)){
                        system("mkdir -p ".$sqlDir);
                    }

                    $sqlFile = sprintf("%s/%s.%s.%s.sql", $sqlDir, $dbName, $tableName, date("Y-m-d-H-i-s"));

                    if (!$handle = fopen($sqlFile, 'w')) {
                        continue;
                    }

                    print_r(sprintf("[%s] import to mysql ===> %s.%s.%s\n", date("H:i:s"), $dbName, $tableName, $date));

                    if ($dbName == "adx_report_sell") {
                        if ($tableName == "media_day") {
                            $arrData = array();
                            foreach ($v3 as $host => $v4) {
                                foreach ($v4 as $mediaId => $v5) {
                                    foreach($v5 as $k => $v) {
                                        $arrData[$mediaId][$k] += $v;
                                    }
                                }
                            }
                            if(empty($arrData)){
                                continue;
                            }
                            foreach($arrData as $mediaId=>$m1){
                                $sellerId = $arrMedia[$mediaId]['sellerId'];
                                $sql = sprintf("delete from %s.%s_%s where mediaId = %d and reportDate = '%s';\n",
                                    $dbName, $tableName, date("Y_m", $time), $mediaId , date("Y-m-d", $time));
                                fwrite($handle, $sql);
                                $temp = 0;
                                $sql  = sprintf("insert into %s.%s_%s(sellerId,mediaId,reportDate,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                    $dbName,
                                    $tableName,
                                    date("Y_m", $time),
                                    intval($sellerId),
                                    intval($mediaId),
                                    date("Y-m-d", $time),
                                    intval($m1['view']),
                                    intval($m1["request"]),
                                    intval($m1["requestOk"]),
                                    intval($m1["response"]),
                                    intval($m1["bid"]),
                                    intval($m1["play"]),
                                    intval($m1["click"]),
                                    intval($m1["spend"]),
                                    intval($m1["sellerPlay"]),
                                    intval($m1["sellerClick"]),
                                    intval($m1["sellerSpend"]),
                                    intval($m1["buyerSpend"]),
                                    intval($m1["bidOk"])
                                );
                                fwrite($handle, $sql);
                            }
                            reset($arrData);
                        }else if ($tableName == "place_day") {
                            $arrData = array();
                            foreach ($v3 as $host => $v4) {
                                foreach ($v4 as $placeId => $v5) {
                                    foreach($v5 as $k => $v){
                                        $arrData[$placeId][$k] += $v;
                                    }
                                }
                            }

                            if(empty($arrData)){
                                continue;
                            }

                            foreach($arrData as $placeId=>$m1){
                                $sellerId = $arrPlace[$placeId]['sellerId'];
                                $mediaId = $arrPlace[$placeId]['mediaId'];

                                $sql = sprintf("delete from %s.%s_%s where mediaId = %d and placeId = %d and reportDate = '%s' and sellerId = %d;\n",
                                    $dbName, $tableName, date("Y_m", $time), $mediaId ,$placeId, date("Y-m-d", $time),$sellerId);
                                fwrite($handle, $sql);
                                $temp = 0;
                                $sql  = sprintf("insert into %s.%s_%s(sellerId,mediaId,placeId,reportDate,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                    $dbName,
                                    $tableName,
                                    date("Y_m", $time),
                                    intval($sellerId),
                                    intval($mediaId),
                                    intval($placeId),
                                    date("Y-m-d", $time),
                                    intval($m1['view']),
                                    intval($m1["request"]),
                                    intval($m1["requestOk"]),
                                    intval($m1["response"]),
                                    intval($m1["bid"]),
                                    intval($m1["play"]),
                                    intval($m1["click"]),
                                    intval($m1["spend"]),
                                    intval($m1["sellerPlay"]),
                                    intval($m1["sellerClick"]),
                                    intval($m1["sellerSpend"]),
                                    intval($m1["buyerSpend"]),
                                    intval($m1["bidOk"])
                                );
                                fwrite($handle, $sql);
                            }
                            reset($arrData);
                        }else if ($tableName == "summary_day") {
                            $arrData = array();
                            foreach ($v3 as $host => $v4) {
                                foreach ($v4 as $sellerId => $v5) {
                                    foreach($v5 as $k => $v){
                                        $arrData[$sellerId][$k] += $v;
                                    }
                                }
                            }
                            if(empty($arrData)){
                                continue;
                            }
                            foreach($arrData as $sellerId=>$m1){
                                $sql = sprintf("delete from %s.%s_%s where reportDate = '%s' and sellerId = %d;\n",
                                    $dbName, $tableName, date("Y_m", $time), date("Y-m-d", $time),$sellerId);
                                fwrite($handle, $sql);
                                $sql  = sprintf("insert into %s.%s_%s(sellerId,reportDate,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                    $dbName,
                                    $tableName,
                                    date("Y_m", $time),
                                    intval($sellerId),
                                    date("Y-m-d", $time),
                                    intval($m1['view']),
                                    intval($m1["request"]),
                                    intval($m1["requestOk"]),
                                    intval($m1["response"]),
                                    intval($m1["bid"]),
                                    intval($m1["play"]),
                                    intval($m1["click"]),
                                    intval($m1["spend"]),
                                    intval($m1["sellerPlay"]),
                                    intval($m1["sellerClick"]),
                                    intval($m1["sellerSpend"]),
                                    intval($m1["buyerSpend"]),
                                    intval($m1["bidOk"])
                                );
//                                print_r($sql);
                                fwrite($handle, $sql);
                            }
                            reset($arrData);
                        }else if ($tableName == "size_day") {
                            $arrData = array();
                            foreach ($v3 as $host => $v4) {
                                foreach ($v4 as $sellerId => $v5) {
                                    foreach($v5 as $w => $v6) {
                                        foreach ($v6 as $h => $v7) {
                                            foreach($v7 as $k => $v){
                                                if($k != "w" && $k != "h"){
                                                    $arrData[$sellerId][$w][$h][$k] += $v;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if(empty($arrData)){
                                continue;
                            }
                            foreach($arrData as $sellerId=>$m1){
                                foreach($m1 as $w=>$m2){
                                    foreach($m2 as $h=>$m1){
                                        $sql = sprintf("delete from %s.%s_%s where reportDate = '%s' and sellerId = %d and w=%d and h=%d;\n",
                                            $dbName, $tableName, date("Y_m", $time), date("Y-m-d", $time),$sellerId,$w,$h);
                                        fwrite($handle, $sql);
                                        $sql  = sprintf("insert into %s.%s_%s(w,h,sellerId,reportDate,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                            $dbName,
                                            $tableName,
                                            date("Y_m", $time),
                                            intval($w),
                                            intval($h),
                                            intval($sellerId),
                                            date("Y-m-d", $time),
                                            intval($m1['view']),
                                            intval($m1["request"]),
                                            intval($m1["requestOk"]),
                                            intval($m1["response"]),
                                            intval($m1["bid"]),
                                            intval($m1["play"]),
                                            intval($m1["click"]),
                                            intval($m1["spend"]),
                                            intval($m1["sellerPlay"]),
                                            intval($m1["sellerClick"]),
                                            intval($m1["sellerSpend"]),
                                            intval($m1["buyerSpend"]),
                                            intval($m1["bidOk"])
                                        );
                                        //print_r($sql);
                                        fwrite($handle, $sql);
                                    }
                                }
                            }
                            reset($arrData);
                        }
                    }else if ($dbName == "adx_report_buy") {
                        if ($tableName == "summary_day") {
                            $arrData = array();
                            foreach ($v3 as $host => $v4) {
                                foreach ($v4 as $buyerId => $v6) {
                                    foreach ($v6 as $k => $v) {
                                        $arrData[$buyerId][$k] += $v;
                                    }
                                }
                            }

                            if (empty($arrData)) {
                                continue;
                            }
                            foreach ($arrData as $buyerId => $val) {
                                $sql = sprintf("delete from %s.%s_%s where buyerId = %d and reportDate = '%s';\n",
                                    $dbName, $tableName, date("Y_m", $time), $buyerId, date("Y-m-d", $time));
                                fwrite($handle, $sql);
                                $temp = 0;
                                $sql = sprintf("insert into %s.%s_%s(buyerId,reportDate,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                    $dbName,
                                    $tableName,
                                    date("Y_m", $time),
                                    intval($buyerId),
                                    date("Y-m-d", $time),
                                    intval($val["request"]),
                                    intval($val["requestOk"]),
                                    intval($val["response"]),
                                    intval($val["bid"]),
                                    intval($val["play"]),
                                    intval($val["click"]),
                                    intval($val["spend"]),
                                    intval($val["sellerPlay"]),
                                    intval($val["sellerClick"]),
                                    intval($val["sellerSpend"]),
                                    intval($val["buyerSpend"]),
                                    intval($val["bidOk"])
                                );
                                fwrite($handle, $sql);
                            }
                            reset($arrData);
                        }
                    }else if ($dbName == "adx_report") {
                        if ($tableName == "summary_day") {
                            $arrData = array();
                            foreach ($v3 as $host => $v4) {
                                foreach ($v4 as $sellerId => $v5) {
                                    if($sellerId){
                                        foreach ($v5 as $buyerId => $v6) {
                                            foreach ($v6 as $k => $v) {
                                                $arrData[$sellerId][$buyerId][$k] += $v;
                                            }
                                        }
                                    }
                                }
                            }

                            if (empty($arrData)) {
                                continue;
                            }
                            foreach ($arrData as $sellerId => $m1) {
                                foreach ($m1 as $buyerId => $m2) {
                                    $sql = sprintf("delete from %s.%s_%s where buyerId = %d and sellerId=%d and reportDate = '%s';\n",
                                        $dbName, $tableName, date("Y_m", $time), $buyerId,$sellerId, date("Y-m-d", $time));
                                    fwrite($handle, $sql);
                                    $temp = 0;
                                    $sql = sprintf("insert into %s.%s_%s(sellerId,buyerId,reportDate,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                        $dbName,
                                        $tableName,
                                        date("Y_m", $time),
                                        intval($sellerId),
                                        intval($buyerId),
                                        date("Y-m-d", $time),
                                        intval($m2["view"]),
                                        intval($m2["request"]),
                                        intval($m2["requestOk"]),
                                        intval($m2["response"]),
                                        intval($m2["bid"]),
                                        intval($m2["play"]),
                                        intval($m2["click"]),
                                        intval($m2["spend"]),
                                        intval($m2["sellerPlay"]),
                                        intval($m2["sellerClick"]),
                                        intval($m2["sellerSpend"]),
                                        intval($m2["buyerSpend"]),
                                        intval($m2["bidOk"])
                                    );
                                    fwrite($handle, $sql);
                                }
                            }
                            reset($arrData);
                        }else if ($tableName == "place_day") {
                            $arrData = array();
                            foreach ($v3 as $host => $v4) {
                                foreach ($v4 as $placeId => $v5) {
                                    foreach ($v5 as $buyerId => $v6) {
                                        foreach ($v6 as $k => $v) {
                                            $arrData[$placeId][$buyerId][$k] += $v;
                                        }
                                    }
                                }
                            }

                            if (empty($arrData)) {
                                continue;
                            }

                            foreach ($arrData as $placeId => $m1) {
                                foreach ($m1 as $buyerId => $m2) {

                                    $sellerId = $arrPlace[$placeId]['sellerId'];
                                    $mediaId  = $arrPlace[$placeId]['mediaId'];

                                    $sql = sprintf("delete from %s.%s_%s where buyerId = %d and placeId = %d and reportDate = '%s';\n",
                                        $dbName, $tableName, date("Y_m", $time), $buyerId,$placeId, date("Y-m-d", $time));
                                    fwrite($handle, $sql);
                                    $sql = sprintf("insert into %s.%s_%s(sellerId,	mediaId,placeId,buyerId,reportDate,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,%s,%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                        $dbName,
                                        $tableName,
                                        date("Y_m", $time),
                                        intval($sellerId),
                                        intval($mediaId),
                                        intval($placeId),
                                        intval($buyerId),
                                        date("Y-m-d", $time),
                                        intval($m2["view"]),
                                        intval($m2["request"]),
                                        intval($m2["requestOk"]),
                                        intval($m2["response"]),
                                        intval($m2["bid"]),
                                        intval($m2["play"]),
                                        intval($m2["click"]),
                                        intval($m2["spend"]),
                                        intval($m2["sellerPlay"]),
                                        intval($m2["sellerClick"]),
                                        intval($m2["sellerSpend"]),
                                        intval($m2["buyerSpend"]),
                                        intval($m2["bidOk"])
                                    );
                                    fwrite($handle, $sql);
                                }
                            }
                            reset($arrData);
                        }else if ($tableName == "place_hour") {
                            $arrData = array();
                            foreach ($v3 as $hour => $v4) {
                                foreach ($v4 as $placeId => $v5) {
                                    foreach ($v5 as $buyerId => $v6) {

                                        if($arrResult[$dbName][$tableName][$date][$hour][$placeId][$buyerId]["update"] == false){
                                            continue;
                                        }

                                        foreach ($v6 as $host => $v7) {
                                            foreach ($v7 as $k => $v) {
                                                $arrData[$hour][$placeId][$buyerId][$k] += $v;
                                            }
                                        }
                                    }
                                }
                            }

                            if (empty($arrData)) {
                                continue;
                            }

                            foreach($arrData as $hour=>$m1) {
                                foreach ($m1 as $placeId => $m2) {
                                    foreach ($m2 as $buyerId => $m3) {
                                        $sellerId = $arrPlace[$placeId]['sellerId'];

                                        if($sellerId == 100149 || $sellerId == 100144){
                                            continue;
                                        }

                                        $mediaId  = $arrPlace[$placeId]['mediaId'];
                                        $sql = sprintf("delete from %s.%s_%s where ihour = %d and  buyerId = %d and sellerId=%d and placeId = %d and reportDate = '%s';\n",
                                            $dbName, $tableName, date("Y_m", $time),$hour, $buyerId,$sellerId,$placeId, date("Y-m-d", $time));
                                        fwrite($handle, $sql);
                                        $temp = 0;
                                        $sql = sprintf("insert into %s.%s_%s(sellerId,	mediaId,placeId,buyerId,reportDate,ihour,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,%s,%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                            $dbName,
                                            $tableName,
                                            date("Y_m", $time),
                                            intval($sellerId),
                                            intval($mediaId),
                                            intval($placeId),
                                            intval($buyerId),
                                            date("Y-m-d", $time),
                                            intval($hour),
                                            intval($m3["view"]),
                                            intval($m3["request"]),
                                            intval($m3["requestOk"]),
                                            intval($m3["response"]),
                                            intval($m3["bid"]),
                                            intval($m3["play"]),
                                            intval($m3["click"]),
                                            intval($m3["spend"]),
                                            intval($m3["sellerPlay"]),
                                            intval($m3["sellerClick"]),
                                            intval($m3["sellerSpend"]),
                                            intval($m3["buyerSpend"]),
                                            intval($m3["bidOk"])
                                        );
                                        fwrite($handle, $sql);
                                    }
                                }
                            }
                            reset($arrData);
                        }else if ($tableName == "failure_day") {
                            $arrData = array();
                            foreach ($v3 as $host => $v4) {
                                foreach ($v4 as $buyerId => $v5) {
                                    foreach ($v5 as $placeId => $v6) {
                                        $sellerId = $arrPlace[$placeId]['sellerId'];
                                        $mediaId  = $arrPlace[$placeId]['mediaId'];
                                        foreach ($v6 as $crId => $v7) {
                                            foreach ($v7 as $errId => $v8) {
                                                foreach ($v8 as $k => $v) {
                                                    $arrData[$buyerId][$sellerId][$mediaId][$placeId][$crId][$errId][$k] += $v;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if (empty($arrData)) {
                                continue;
                            }
                            foreach ($arrData as $buyerId => $m1) {
                                foreach ($m1 as $sellerId => $m2) {
                                    foreach ($m2 as $mediaId => $m3) {
                                        foreach ($m3 as $placeId => $m4) {
                                            foreach ($m4 as $crId => $m5) {
                                                foreach ($m5 as $errId => $m6) {
                                                    $sql = sprintf("delete from %s.%s_%s where buyerId = %d and creativeId = %d and sellerId=%d and placeId = %d and mediaId=%d and errorId=%d and reportDate = '%s';\n",
                                                        $dbName, $tableName, date("Y_m", $time), $buyerId,$crId,$sellerId,$placeId,$mediaId,$errId, date("Y-m-d", $time));
                                                    fwrite($handle, $sql);
                                                    $temp = 0;
                                                    $sql = sprintf("insert into %s.%s_%s(`reportDate`,`buyerId`,`creativeId`,`sellerId`,`mediaId`,`placeId`,`errorId`,`errorTotal`)values('%s',%s,%s,%s,%s,%s,%s,%s);\n",
                                                        $dbName,
                                                        $tableName,
                                                        date("Y_m", $time),
                                                        date("Y-m-d", $time),
                                                        intval($buyerId),
                                                        intval($crId),
                                                        intval($sellerId),
                                                        intval($mediaId),
                                                        intval($placeId),
                                                        intval($errId),
                                                        intval($m6["errorTotal"])
                                                    );
                                                    fwrite($handle, $sql);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            reset($arrData);
                        }
                    }
                    fclose($handle);

                    $cmd = sprintf("%s -h%s -u%s -p%s %s --default-character-set utf8 < %s ",
                        $arrCfg['db']['mysqlBin'],
                        $arrCfg['db']['host'],
                        $arrCfg['db']['user'],
                        $arrCfg['db']['pwd'],
                        $arrCfg['db']['name'],
                        $sqlFile);
//                    print_R("----------------------\r\n");
//                    print_R($cmd."\r\n");
//                    print_R("----------------------\r\n");
                    system($cmd);
                }
            }
        }
//      break;
    }
}

if (!empty($arrFinishFiles)){
//	print_r($arrFinishFiles);
    foreach ($arrFinishFiles as $logFile) {
        if (file_exists($logFile)){
            unlink($logFile);
        }
    }
}

if (file_exists($pidFile)){
    unlink($pidFile);
}

$useTime = time()-$startRunTime;
print_r(sprintf("[%s] ---------- import to db, used $useTime second, finished.\n", date("H:i:s"), $useTime));

die();


/**
 *  函数:
 *  描述: 将有变化的数据，汇总的结果存入文件中
 *  输入:
 *  输出:
 **/
function writeCache($dbName, $tableName, $date, $data)
{
    global $tmpPath;

    if (empty($data)) {
        return true;
    }

    if (!is_dir($tmpPath)) {
        system("mkdir -p " . $tmpPath);
        system("chmod -R 755 " . $tmpPath);
    }

    $time = strtotime($date);

    $cacheDir = sprintf("%s/%s", $tmpPath, date("Ym/d", $time));
    if (!file_exists($cacheDir)) {
        system("mkdir -p " . $cacheDir);
    }

    $cacheFile = sprintf("%s/%s.%s.%s", $cacheDir, $dbName, $tableName, $date);

    print_r(sprintf("[%s] writeCache %s %s %s\n", date("H:i:s"), $dbName, $tableName, $date));

    file_put_contents($cacheFile, json_encode($data)."\n");

    return true;
}

/**
 *  函数:
 *  描述: 将缓存数据加载到内存中
 *  输入:
 *  输出:
 **/
function loadCache($dbName, $tableName, $date)
{
    global $tmpPath, $arrResult;

    $time = strtotime($date);
    $cacheFile = sprintf("%s/%s/%s/%s.%s.%s",
        $tmpPath, date("Ym", $time), date("d", $time), $dbName, $tableName, $date);

    if (!file_exists($cacheFile)) {
        return false;
    }

    $json = file_get_contents($cacheFile);
    $cache = json_decode($json, true);

    if (empty($cache)) {
        return false;
    }

    print_r(sprintf("[%s] loadCache %s %s %s\n", date("H:i:s"), $dbName, $tableName, $date));

    if($dbName == "adx_report_sell"){
        if ($tableName == "media_day") {
            foreach ($cache as $host => $v1) {
                foreach ($v1 as $placeId => $v2) {
                    foreach($v2 as $k=>$v){
                        $arrResult[$dbName][$tableName][$date][$host][$placeId][$k] = $v;
                    }
                }
            }
        } else if ($tableName == "media_hour") {
            foreach ($cache as $host => $v1) {
                foreach ($v1 as $hour => $v2) {
                    foreach ($v2 as $mediaId => $v3) {
                        foreach ($v3 as $tagid => $v4) {
                            foreach ($v4 as $size => $v5) {
                                foreach ($v5 as $k => $v) {
                                    $arrResult[$dbName][$tableName][$date][$host][$hour][$mediaId][$tagid][$size][$k] = $v;
                                }
                            }
                        }
                    }
                }
            }
        } else if ($tableName == "place_day") {
            foreach ($cache as $host => $v1) {
                foreach ($v1 as $placeId => $v2) {
                    foreach($v2 as $k=>$v){
                        $arrResult[$dbName][$tableName][$date][$host][$placeId][$k] = $v;
                    }
                }
            }
        }else if ($tableName == "summary_day") {
            foreach ($cache as $host => $v1) {
                foreach ($v1 as $sellerId => $v2) {
                    foreach($v2 as $k=>$v){
                        $arrResult[$dbName][$tableName][$date][$host][$sellerId][$k] = $v;
                    }
                }
            }
        }else if ($tableName == "size_day") {
            foreach ($cache as $host => $v1) {
                foreach ($v1 as $sellerId => $v2) {
                    foreach($v2 as $w=> $val3) {
                        foreach($val3 as $h=> $val4) {
                            foreach($val4 as $k => $v){
                                $arrResult[$dbName][$tableName][$date][$host][$sellerId][$w][$h][$k] = $v;
                            }
                        }
                    }
                }
            }
        }
    }else if($dbName == "adx_report_buy"){
        if ($tableName == "summary_day") {
            foreach ($cache as $host => $v1) {
                    foreach($v1 as $buyerId => $val) {
                        foreach($val as $k=>$v){
                            $arrResult[$dbName][$tableName][$date][$host][$buyerId][$k] = $v;
                        }
                    }
            }
        }
    }else if($dbName == "adx_report"){
        if ($tableName == "summary_day") {
            foreach ($cache as $host => $v1) {
                foreach ($v1 as $sellerId => $v2) {
                    foreach($v2 as $buyerId => $val) {
                        foreach($val as $k=>$v){
                            $arrResult[$dbName][$tableName][$date][$host][$sellerId][$buyerId][$k] = $v;
                        }
                    }
                }
            }
        }elseif ($tableName == "place_day") {
            foreach ($cache as $host => $v1) {
                foreach ($v1 as $placeId => $v2) {
                    foreach($v2 as $buyerId => $val) {
                        foreach($val as $k=>$v){
                            $arrResult[$dbName][$tableName][$date][$host][$placeId][$buyerId][$k] = $v;
                        }
                    }
                }
            }
        }elseif ($tableName == "place_hour") {
            foreach ($cache as $hour => $v1) {
                foreach ($v1 as $placeId => $v2) {
                    foreach($v2 as $buyerId => $v3) {
                        $arrResult[$dbName][$tableName][$date][$hour][$placeId][$buyerId]["update"] = false;
                        foreach ($v3 as $host => $v4) {
                            foreach($v4 as $k=>$v){
                                $arrResult[$dbName][$tableName][$date][$hour][$placeId][$buyerId][$host][$k] = $v;
                            }
                        }
                    }
                }
            }
        }else if ($tableName == "failure_day") {
            foreach ($cache as $host => $v1) {
                foreach($v1 as $buyerId => $v2) {
                    foreach ($v2 as $placeId => $v3) {
                        foreach($v3 as $crId => $v4) {
                            foreach($v4 as $errId=>$v5) {
                                foreach ($v5 as $k => $v) {
                                    $arrResult[$dbName][$tableName][$date][$host][$buyerId][$placeId][$crId][$errId][$k] = $v;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return true;
}