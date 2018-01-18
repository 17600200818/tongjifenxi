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

/*$y = "2014";
$month = '9';
for($i = 0;$i<40;$i++){
    $month = $month<=9?"0".$month:$month;
    $tab = "dsp_media_day_".$y."_".$month;
    $sql = 'SELECT id,dspid,mediaid,adplaceid,reportdate,view,play,click,spend,size,request,requestsend,response,responsead,success FROM '.$tab;




    $result = $db1->query ( $sql );
    $rs = $result->fetchAll();
    $month++;
    if($month ==12){
        $month = 1;
        $y++;
    }

    if(){

    }
}*/
$arrResult = array();

if(!empty($rs)) {
    foreach ($rs as $k=>$v) {
        $date = $v['reportdate'];
        $sql2 = "SELECT uid FROM ex_main.`media` where id= ".$v['mediaid'];
        $result2 = $db1->query ( $sql2 );
        $rs2 = $result2->fetchAll();
        if($rs2){
            $sellerId = $rs2[0]['uid'];
            $buyerId  = $v['dspid'];

            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["view"]             +=$v['view'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["play"]             +=$v['play'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["click"]            +=$v['click'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["spend"]            +=$v['spend'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["request"]          +=$v['request'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["requestOk"]        +=$v['requestsend'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["response"]         +=$v['response'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["bid"]              +=$v['responsead'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["bidOk"]            +=$v['success'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["sellerPlay"]       +=$v['play'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["sellerClick"]      +=$v['click'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["sellerSpend"]      +=$v['spend'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["buyerSpend"]       +=$v['spend'];
            $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["reportDate"]       =$v['reportdate'];
        }
    }
        $time = strtotime($date);

        $out= "/home/rtb/statV2/tmp/updateReportData/result";
        $sqlDir	= sprintf("%s/%s", $out, date("Ym", $time));
        if (!file_exists($sqlDir)){
            system("mkdir -p ".$sqlDir);
        }

        $sqlFile = sprintf("%s/%s.%s.%s.sql", $sqlDir, "adx_report", "summary_day", date("Y-m-d-H-i-s"));

        if (!$handle = fopen($sqlFile, 'w')) {
            print_r("\r\n结束\r\n");
        }

        foreach ($arrResult as $dbName => $m1) {
            foreach ($m1 as $tableName => $m2) {
                foreach ($m2 as $date => $m3) {
                    foreach ($m3 as $sellerId => $m4) {
                        foreach ($m4 as $buyerId => $m2) {
                            $sql = sprintf("insert into %s.%s_%s(sellerId,buyerId,reportDate,`view`,request,requestOk,response,bid,play,click,spend,sellerPlay,sellerClick,sellerSpend,buyerSpend,bidOk)values(%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n",
                                $dbName,
                                $tableName,
                                date("Y_m", $time),
                                intval($sellerId),
                                intval($buyerId),
                                $m2['reportDate'],
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



if (file_exists($pidFile)){
    unlink($pidFile);
}

$useTime = time()-$startRunTime;
print_r(sprintf("[%s] ---------- import to db, used $useTime second, finished.\n", date("H:i:s"), $useTime));

die();