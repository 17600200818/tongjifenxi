<?php

define("REDIS_EXPIRE", 86400);

/**
 *  函数:
 *  描述: 更新redis数据，默认保存15天
 *  输入:
 *  输出:
 **/
function updateRedisData($type, $cookie, $hashName, $key, $value, $timeout=REDIS_EXPIRE)
{
    global $redisList;
    static $arrRedis;

    if (!isset($redisList) || count($redisList) < 1){
        return false;
    }

    if(empty($key)){
        return false;
    }

    $index = crc32($key) % count($redisList);

    $redisInfo = $redisList[$index];

    if (!isset($arrRedis[$redisInfo])){
        $arrInfo = explode(":", $redisInfo);

        $arrRedis[$redisInfo] = new Redis();
        $arrRedis[$redisInfo]->connect($arrInfo[0], $arrInfo[1]);
    }

    if ($type == 'SET'){
        $arrRedis[$redisInfo]->setex($key, $timeout, $value);
    }
    elseif ($type == 'HASH'){
        $arrRedis[$redisInfo]->hSet($hashName, $key, $value);
        $arrRedis[$redisInfo]->setTimeout($hashName, $timeout);
    }
    else{
        return false;
    }

    return true;
}

/**
 *  函数:
 *  描述: 得到redis数据
 *  输入:
 *  输出:
 **/
function getRedisData($type, $cookie, $hashName, $key=null)
{
    global $redisList;
    static $arrRedis;

    if (!isset($redisList) || count($redisList) < 1){
        return false;
    }

    if(empty($key)){
        return false;
    }

    $index = crc32($key) % count($redisList);

    $redisInfo = $redisList[$index];

    if (!isset($arrRedis[$redisInfo])){
        $arrInfo = explode(":", $redisInfo);

        $arrRedis[$redisInfo] = new Redis();
        $arrRedis[$redisInfo]->connect($arrInfo[0], $arrInfo[1]);
    }

    if ($type == 'SET'){
        if (empty($key)){
            return false;
        }

        return $arrRedis[$redisInfo]->get($key);
    }
    elseif ($type == 'HASH'){
        if (empty($key)){
            return $arrRedis[$redisInfo]->hGetAll($hashName);
        }
        else{
            return $arrRedis[$redisInfo]->hGet($hashName, $key);
        }
    }
    else{
        return false;
    }
}

/**
 *  函数:
 *  描述: 删除redis数据
 *  输入:
 *  输出:
 **/
function delRedisData($type, $cookie, $hashName, $key=null)
{
    global $redisList;
    static $arrRedis;

    if (!isset($redisList) || count($redisList) < 1){
        return false;
    }

    if(empty($key)){
        return false;
    }

    $index = crc32($key) % count($redisList);

    $redisInfo = $redisList[$index];

    if (!isset($arrRedis[$redisInfo])){

        $arrInfo = explode(":", $redisInfo);

        $arrRedis[$redisInfo] = new Redis();

        $arrRedis[$redisInfo]->connect($arrInfo[0], $arrInfo[1]);
    }

    if ($type == 'SET'){
        return $arrRedis[$redisInfo]->delete($key);
    }
    elseif ($type == 'HASH'){
        if (empty($key)){
            return $arrRedis[$redisInfo]->setTimeout($hashName, 1);
        }
        else{
            return $arrRedis[$redisInfo]->hDel($hashName, $key);
        }
    }
    else{
        return false;
    }
}
