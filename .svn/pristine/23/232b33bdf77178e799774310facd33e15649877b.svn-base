<?php
ini_set('error_reporting',		'E_ALL');
ini_set('max_execution_time',	'0');
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

    $bakDir = sprintf("../tmp/dbBak/%s", date("Y-m-d"));
    if(!file_exists($bakDir)){
        $cmd = sprintf("mkdir -p %s", $bakDir);
        system($cmd);

        foreach($rs as $v) {
            $dbName = trim($v["Database"]);

            $arrFilter = array("mysql", "test", "information_schema", "performance_schema");

            if (in_array($dbName, $arrFilter)) {
                continue;
            }

            $cmd = sprintf("/usr/local/bin/mysqldump --opt -h%s -u%s -p%s %s |gzip > %s/%s.sql.gz", $arrCfg['db']['host'], $arrCfg['db']['user'], $arrCfg['db']['pwd'], $dbName, $bakDir, $dbName);
            print_R(sprintf("----- %s\n", $cmd));
            exec($cmd);
        }
    }
}

$oldDir = sprintf("./bak/%s", date("Y-m-d", time(NULL)-10*24*60*60));
if(file_exists($oldDir)){
    $cmd = sprintf("rm -rf %s", $oldDir);
    system($cmd);
}

if (file_exists($pidFile)){
    unlink($pidFile);
}

$useTime = time()-$startRunTime;
print_r(sprintf("[%s] ---------- backup database, used $useTime second, finished.\n", date("H:i:s"), $useTime));

die();

