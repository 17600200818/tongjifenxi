<?php

require_once 'Zend/Db.php';

$arrPlace = array();

function loadParam(){
    global $arrCfg, $arrPlace, $arrCreative;

    $params = array (
        'host' => $arrCfg["db"]["host"],
        'username' => $arrCfg["db"]["user"],
        'password' => $arrCfg["db"]["pwd"],
        'dbname' => $arrCfg["db"]["name"],
        'charset' => 'utf8'
    );

    $db = Zend_Db::factory ( 'PDO_MYSQL', $params );

    //  读取广告位的参数信息
    $sql = 'SELECT id, sellerId, sellerSonId, mediaId, width, height,mediaPlaceId FROM  `place`';
    $result = $db->query ( $sql );
    $rs = $result->fetchAll();

    if(!empty($rs)) {
        foreach ($rs as $item) {
			$id = intval($item["id"]);
            $sellerId = intval($item["sellerId"]);
            $sellerSonId = intval($item["sellerSonId"]);
            $mediaId = intval($item["mediaId"]);
            $width = intval($item["width"]);
            $height = intval($item["height"]);

            $arrPlace[$id]["id"] = $id;
            $arrPlace[$id]["sellerId"] = $sellerId;
            $arrPlace[$id]["sellerSonId"] = $sellerSonId;
            $arrPlace[$id]["mediaId"] = $mediaId;
            $arrPlace[$id]["w"] = $width;
            $arrPlace[$id]["h"] = $height;
        }
    }


    //  读取素材信息
    $sql = 'SELECT id, idBuyer, buyerCrid, url FROM  `creative`';
    $result = $db->query ( $sql );
    $rs = $result->fetchAll();

    if(!empty($rs)) {
        foreach ($rs as $item) {
            $id = intval($item["id"]);
            $buyerId = intval($item["idBuyer"]);
            $buyerCrid = trim($item["buyerCrid"]);
            $url = trim($item["url"]);

            $key = sprintf("%d_%s", $buyerId, $buyerCrid);
            $arrCreative[$key]["id"] = $id;
            $arrCreative[$key]["buyerId"] = $buyerId;

            $key = sprintf("%d_%s", $buyerId, $url);
            $arrCreative[$key]["id"] = $id;
            $arrCreative[$key]["buyerId"] = $buyerId;
        }
    }
}
