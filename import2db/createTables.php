<?php
ini_set('error_reporting',		'E_ALL');
ini_set('max_execution_time',	'60');
ini_set("memory_limit",			"-1");
ini_set('display_errors',		'on');
ini_set('log_errors',			'off');

//根目录
define('BASE_PATH', dirname(__FILE__));
set_include_path(implode(PATH_SEPARATOR, array(BASE_PATH, BASE_PATH."/../lib/", get_include_path())));

require_once("mysql.php");
require_once("comm.php");
require_once("redis.lib.php");

$startRunTime = time();

$serviceFile = __FILE__;
$pathParts	 = pathinfo($serviceFile);
$serviceName = str_ireplace('.php', '', $pathParts['basename']);

$pidFile = sprintf("%s/%s.pid", BASE_PATH, $serviceName);

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

checkServiceExists();

$logPath	= sprintf("%s/../%s", BASE_PATH, $arrCfg['import2db']['logpath']);
$outPath	= sprintf("%s/../%s", BASE_PATH, $arrCfg['import2db']['outpath']);
$tmpPath	= sprintf("%s/../%s", BASE_PATH, $arrCfg['import2db']['tmppath']);
$errPath	= sprintf("%s/../%s", BASE_PATH, $arrCfg['import2db']['errpath']);

if (!is_dir($logPath)){
    system("mkdir -p ".$logPath);
    system("chmod -R 755 ".$logPath);
}

$objDb = new Mysql();
$objDb->Host		= $arrCfg['db']['host'];
$objDb->Database	= $arrCfg['db']['name'];
$objDb->User		= $arrCfg['db']['user'];
$objDb->Password	= $arrCfg['db']['pwd'];
$objDb->charset	    = 'utf8';

if($objDb->connect() === false){
    print_r(sprintf("[%s] connect db [%s:%s] error.\n", date("H:i:s"),
        $arrCfg['db']['host'], $arrCfg['db']['name']));

    if (file_exists($pidFile)){
        unlink($pidFile);
    }

    die();
}

$sql = "show databases;";
$objDb->query ( $sql );
$rs = $objDb->fetchAll();
$objDb->close();

if(!empty($rs)) {
    foreach($rs as $v) {
        $dbName = trim($v["Database"]);

        $arrFilter = array("mysql", "test", "information_schema", "performance_schema");

        if(in_array($dbName, $arrFilter)){
            continue;
        }

//        print_R(sprintf("dbName : %s\n", $dbName));
        $objDb = new Mysql();
        $objDb->Host		= $arrCfg['db']['host'];
        $objDb->Database	= $dbName;
        $objDb->User		= $arrCfg['db']['user'];
        $objDb->Password	= $arrCfg['db']['pwd'];
        $objDb->charset	    = 'utf8';

        $sql = sprintf("show tables;");
        $objDb->query($sql);
        $arrRs = $objDb->fetchAll();

        if (!empty($arrRs)){

            $arrTable = array();

            $tableExt = sprintf("/_%04d_%02d/", date("Y"), date("m"));

            foreach ($arrRs as $rs) {
                foreach ($rs as $tableName) {

//                    $rtn = preg_match("/_20(\d{2})_(\d{2})/", $tableName, $match);
                    $rtn = preg_match($tableExt, $tableName, $match);

                    if (!$rtn){
                        continue;
                    }

                    $arrTableName = explode("_", $tableName);
                    if (empty($arrTableName) || count($arrTableName) < 3){
                        continue;
                    }

                    $count = count($arrTableName);
                    $arrPrefix = array();
                    foreach ($arrTableName as $key=>$val) {
                        if ($key < ($count-2)){
                            $arrPrefix[] = $val;
                        }
                    }

                    $prefix = implode("_", $arrPrefix);

                    if (!isset($arrTable[$prefix])){
                        $arrTable[$prefix] = $tableName;
                    }
                }
            }

            if (!empty($arrTable)){
//		print_r($arrTable);

                foreach ($arrTable as $prefix=>$template) {
                    for ($i=1; $i<4; $i++) {
                        $time = mktime(1, 1, 1, date("m")+$i, date("d"), date("Y"));
                        $sql = sprintf("create table IF NOT EXISTS %s_%s like %s;\n", $prefix, date("Y_m", $time), $template);
                        print_r($sql);
                        $objDb->query($sql);
                    }
                }
            }

        }

        $objDb->close();
    }
}

if (file_exists($pidFile)){
    unlink($pidFile);
}

$useTime = time()-$startRunTime;
print_r(sprintf("[%s] ---------- create tables, used $useTime second, finished.\n", date("H:i:s"), $useTime));

die();

