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

//线上
$params1 = array (
    'host' => "web01.rtbs.cn",
    'username' => "statV2Read",
    'password' => "rtbs2017",
    'dbname' => "report_dspmedia",
    'charset' => 'utf8'
);

//沙箱
$params2 = array (
    'host' => "localhost",
    'username' => "adx",
    'password' => "windows2000",
    'dbname' => "report_dspmedia",
    'charset' => 'utf8'
);

$db1 = Zend_Db::factory ( 'PDO_MYSQL', $params1 );
$db2 = Zend_Db::factory ( 'PDO_MYSQL', $params2 );

$y = "2017";
$month = '06';
$tab = "dsp_media_day_".$y."_".$month;
$sql = 'SELECT dspid,mediaid,adplaceid,reportdate,view,play,click,spend,size,request,requestsend,response,responsead,success FROM '.$tab;
$result = $db1->query ( $sql );
$rs = $result->fetchAll();
print_r("\r\n---------------1------------\r\n");

$arrResult = array();
$arrPlace=array();
if(!empty($rs)) {
    foreach ($rs as $k=>$v) {
        $date       = $v['reportdate'];
        $buyerId    = $v['dspid'];
        $mediaid    = $v['mediaid'];


        if($mediaid != 186 && $mediaid != 191){
            $placeId  = $v['adplaceid'];
            if(strpos($placeId, "x") === false){

                $sql2 = "SELECT uid FROM ex_main.`media` where id= ".$mediaid;
                $result2 = $db1->query ( $sql2 );
                $rs2 = $result2->fetchAll();
                if($rs2){
                    $sellerId = $rs2[0]['uid'];
                    $arrPlace[$placeId]["sellerId"] = $sellerId;
                    $arrPlace[$placeId]["mediaId"] = $v['mediaid'];

                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["view"]             +=$v['view'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["play"]             +=$v['play'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["click"]            +=$v['click'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["spend"]            +=$v['spend'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["request"]          +=$v['request'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["requestOk"]        +=$v['requestsend'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["response"]         +=$v['response'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["bid"]              +=$v['responsead'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["bidOk"]            +=$v['success'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["sellerPlay"]       +=$v['play'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["sellerClick"]      +=$v['click'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["sellerSpend"]      +=$v['spend'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["buyerSpend"]       +=$v['spend'];
                    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["reportDate"]       =$v['reportdate'];
                }else{
                    //print_r("\r\n------------2222-------------------\r\n");

                }
            }
        }
    }


        foreach ($arrResult as $dbName => $m1) {
            foreach ($m1 as $tableName => $m2) {
                foreach ($m2 as $date => $m3) {
                    $time = strtotime($date);

                    $out= "/home/rtb/statV2/tmp/updatePlaceReportData/result";
                    $sqlDir	= sprintf("%s/%s", $out, date("Ym", $time));
                    if (!file_exists($sqlDir)){
                        system("mkdir -p ".$sqlDir);
                    }

                    $sqlFile = sprintf("%s/%s.%s.%s.sql", $sqlDir, "adx_report", "place_day", date("Y-m-d-H-i-s"));

                    if (!$handle = fopen($sqlFile, 'w')) {
                        print_r("\r\n结束\r\n");
                    }

                    foreach($m3 as $placeId=>$m4){
                        if($placeId>0){
                            foreach($m4 as $buyerId=>$m5){
                                $sellerId = $arrPlace[$placeId]['sellerId'];
                                $mediaId = $arrPlace[$placeId]['mediaId'];
                                $sql = sprintf("delete from %s.%s_%s where buyerId = %d and placeId = %d and reportDate = '%s';\n",
                                    $dbName, $tableName, date("Y_m", $time), $buyerId,$placeId, date("Y-m-d", $time));
                                fwrite($handle, $sql);
                                $sql = sprintf("insert into %s.%s_%s(sellerId,mediaId,placeId,buyerId,reportDate,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,%s,%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                    $dbName,
                                    $tableName,
                                    date("Y_m", $time),
                                    intval($sellerId),
                                    intval($mediaId),
                                    intval($placeId),
                                    intval($buyerId),
                                    date("Y-m-d", $time),
                                    intval($m5["view"]),
                                    intval($m5["request"]),
                                    intval($m5["requestOk"]),
                                    intval($m5["response"]),
                                    intval($m5["bid"]),
                                    intval($m5["play"]),
                                    intval($m5["click"]),
                                    intval($m5["spend"]),
                                    intval($m5["sellerPlay"]),
                                    intval($m5["sellerClick"]),
                                    intval($m5["sellerSpend"]),
                                    intval($m5["buyerSpend"]),
                                    intval($m5["bidOk"])
                                );
                                fwrite($handle, $sql);
                            }
                        }
                    }


                    fclose($handle);

                    $cmd = sprintf("%s -h%s -u%s -p%s %s --default-character-set utf8 < %s ",
                        "mysql",
                        "localhost",
                        "adx",
                        "windows2000",
                        "adexchange",
                        $sqlFile);
                    print_R("----------------------\r\n");
                    print_R($cmd."\r\n");
                    print_R("----------------------\r\n");
                    system($cmd);
                }
            }
        }



}



if (file_exists($pidFile)){
    unlink($pidFile);
}

$useTime = time()-$startRunTime;
print_r(sprintf("[%s] ---------- import to db, used $useTime second, finished.\n", date("H:i:s"), $useTime));

die();