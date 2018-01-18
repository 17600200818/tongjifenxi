<?php

require_once 'Zend/Db.php';

$arrPlace = array();

function loadParam(){
    global $arrCfg, $db, $arrPlace, $arrMedia;

    $params = array (
        'host' => $arrCfg["db"]["host"],
        'username' => $arrCfg["db"]["user"],
        'password' => $arrCfg["db"]["pwd"],
        'dbname' => $arrCfg["db"]["name"],
        'charset' => 'utf8'
    );

    $db = Zend_Db::factory ( 'PDO_MYSQL', $params );

    //  读取广告位的参数信息
    $sql = 'SELECT id, mediaid, width, height, sellerid FROM place';
    $result = $db->query ( $sql );
    $rs = $result->fetchAll();
    if(!empty($rs)) {
        foreach ($rs as $item) {
            $placeId = intval($item["id"]);
            $mid = intval($item["mediaid"]);
            $width = intval($item["width"]);
            $height = intval($item["height"]);
            $sellerId = intval($item["sellerid"]);

            $arrPlace[$placeId]["sellerId"] = $sellerId;
            $arrPlace[$placeId]["mediaId"] = $mid;
            $arrPlace[$placeId]["w"] = $width;
            $arrPlace[$placeId]["h"] = $height;
        }
    }

    //  读取媒体的参数信息
    $sql = 'SELECT * FROM media';
    $result = $db->query ( $sql );
    $rs1 = $result->fetchAll();
    if(!empty($rs1)) {
        foreach ($rs1 as $item) {

            $mediaId = intval($item["id"]);
            $sellerId = intval($item["sellerId"]);

            $arrMedia[$mediaId]["sellerId"] = $sellerId;
        }
    }

}
