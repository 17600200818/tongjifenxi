<?php

ini_set('error_reporting',		'E_ALL');
ini_set('max_execution_time',	'0');
ini_set("memory_limit",			"-1");
ini_set('display_errors',		'on');
ini_set('log_errors',			'on');

define('BASE_PATH', dirname(__FILE__));
set_include_path(implode(PATH_SEPARATOR, array(BASE_PATH, BASE_PATH."/../lib/", get_include_path())));

require_once("db.php");
require_once("comm.php");

$startRunTime = time();

$serviceFile = __FILE__;
$pathParts	 = pathinfo($serviceFile);
$serviceName = str_ireplace('.php', '', $pathParts['basename']);

$pidFile = sprintf("%s/%s.pid", BASE_PATH, $serviceName);

$arrCfg = parse_ini_file(BASE_PATH."/stat.ini", true);

checkServiceExists();

$bakPath = sprintf("%s/../%s", BASE_PATH, $arrCfg['stat']['bakpath']);

if (!is_dir($bakPath)){
    system("mkdir -p ".$bakPath);
    system("chmod -R 755 ".$bakPath);
}

$arrFile = scandir($bakPath);

print_r(sprintf("[%s] scandir: %s\t[%d]\n", date("H:i:s"), $bakPath, count($arrFile)-2));

if (!empty($arrFile) && count($arrFile) > 2) {
    sort($arrFile);
    foreach ($arrFile as $key => $filename) {
        if ($filename == '.' || $filename == '..') {
            continue;
        }
        $logFile = sprintf("%s/%s", $bakPath, $filename);
        $filesize = number_format(filesize($logFile)/1024, 2, '.', '');
        print_r(sprintf("[%s] stat [%02d]: %s\t[%01.2fK]\n", date("H:i:s"), ++$index, $filename, $filesize));

        $tmp = explode(".", $filename);
        if(empty($tmp)){
            continue;
        }

        $tmp2 = explode("-", $tmp[count($tmp)-1]);

        if(count($tmp2)<4){
            continue;
        }

        $year = intval($tmp2[0]);
        $month = intval($tmp2[1]);
        $day = intval($tmp2[2]);
        $hour = intval($tmp2[3]);

        $bakDir = sprintf("%s/%04d%02d/%02d/", $bakPath, $year, $month, $day);

        if(!file_exists($bakDir)){
            $cmd = sprintf("mkdir -p %s", $bakDir);
            system($cmd);
        }

        $cmd = sprintf("mv %s %s", $logFile, $bakDir);
        system($cmd);

        $cmd = sprintf("gzip %s/%s", $bakDir, $filename);
        print_R($cmd."\n");
        system($cmd);
    }
}

if (file_exists($pidFile)){
    unlink($pidFile);
}

$useTime = time()-$startRunTime;
print_r(sprintf("[%s] ---------- used $useTime second, finished.\n", date("H:i:s"), $useTime));

die();
