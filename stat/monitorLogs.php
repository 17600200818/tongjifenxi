<?phprequire_once("db.php");require_once("redis.lib.php");//  5   曝光日志function parseViewLog(&$arrRow){    global $arrResult, $arrPlace;    $buyerId    = intval($arrRow[3]);    $monitor    = json_decode($arrRow[4], true);    if ($monitor == false || empty($monitor)) {        return 5001;    }    $w = intval($monitor['w']);    $h = intval($monitor['h']);    $bidid    = trim($monitor['bidid']);    $impid    = intval($monitor['impid']);    $placeId    = intval($monitor['placeId']);       //广告位ID    $crid	    = intval($monitor['crid']);    if($placeId < 1){        return 5003;    }    if(!isset($arrPlace[$placeId])){        return 5004;    }    if($crid < 1){        return 5005;    }    if($buyerId < 1){        return 5008;    }    $sellerId = intval($arrPlace[$placeId]['sellerId']);    $mediaId = intval($arrPlace[$placeId]['mediaId']);    if($sellerId != 100103 || $buyerId != 100001){      //  壁合定采的流量不去重        $redisKey = sprintf("stat_vm_%s_%d", $bidid, $impid);        $rtn = getRedisData("SET", "", "", $redisKey);        if($rtn == false || empty($rtn)) {            updateRedisData("SET", "", "", $redisKey, 1);        }else{            return 5006;        }    }    $time = intval($monitor['ctime']);  //时间戳    if(date("Y", $time) < 2017){        return 5002;    }    $date = date("Y-m-d", $time);    $hour = date("h", $time);    //  zplay、adview 需要特殊处理    if($sellerId == 100144 || $sellerId == 100149) {        $sellerSpend = intval($monitor['bidPrice'])*10;    }else{        $sellerSpend = intval($monitor['mediaBidfloor'])*10;    }    $buyerSpend	= intval($monitor['bidPrice'])*10;    //卖方    $arrResult['adx_report_sell']["media_day"][$date][$mediaId]["play"] ++;    $arrResult['adx_report_sell']["place_day"][$date][$placeId]["play"] ++;    $arrResult['adx_report_sell']["summary_day"][$date][$sellerId]["play"] ++;    $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["play"] ++;    $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]['w'] = $w;    $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["h"] = $h;    //买方    $arrResult['adx_report_buy']["summary_day"][$date][$buyerId]["play"] ++;    //运营库（report）    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["play"] ++;    $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["play"] ++;    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["spend"] += $buyerSpend;    $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["spend"] += $buyerSpend;    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["buyerSpend"] += $buyerSpend;    $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["buyerSpend"] += $buyerSpend;    if($sellerId != 100144 && $sellerId != 100149) {        $arrResult['adx_report']["place_hour"][$date][$hour][$placeId][$buyerId]["play"]++;        $arrResult['adx_report']["place_hour"][$date][$hour][$placeId][$buyerId]["spend"] += $buyerSpend;        $arrResult['adx_report']["place_hour"][$date][$hour][$placeId][$buyerId]["buyerSpend"] += $buyerSpend;    }    //卖方    $arrResult['adx_report_sell']["media_day"][$date][$mediaId]["spend"] += $buyerSpend;    $arrResult['adx_report_sell']["place_day"][$date][$placeId]["spend"] += $buyerSpend;    $arrResult['adx_report_sell']["summary_day"][$date][$sellerId]["spend"] += $buyerSpend;    $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["spend"] +=$buyerSpend;    $arrResult['adx_report_sell']["media_day"][$date][$mediaId]["buyerSpend"] += $buyerSpend;    $arrResult['adx_report_sell']["place_day"][$date][$placeId]["buyerSpend"] += $buyerSpend;    $arrResult['adx_report_sell']["summary_day"][$date][$sellerId]["buyerSpend"] += $buyerSpend;    $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["buyerSpend"] +=$buyerSpend;    //买方    $arrResult['adx_report_buy']["summary_day"][$date][$buyerId]["spend"] += $buyerSpend;    $arrResult['adx_report_buy']["summary_day"][$date][$buyerId]["buyerSpend"] += $buyerSpend;    //  媒体扣量处理    if(isset($monitor['sellerStatFlag']))        $sellerStatFlag	= boolval($monitor['sellerStatFlag']);    else        $sellerStatFlag = true;    if($sellerStatFlag == true){        $arrResult['adx_report_sell']["media_day"][$date][$mediaId]["sellerPlay"] ++;        $arrResult['adx_report_sell']["place_day"][$date][$placeId]["sellerPlay"] ++;        $arrResult['adx_report_sell']["summary_day"][$date][$sellerId]["sellerPlay"] ++;        $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["sellerPlay"] ++;        $arrResult['adx_report_sell']["media_day"][$date][$mediaId]["sellerSpend"] += $sellerSpend;        $arrResult['adx_report_sell']["place_day"][$date][$placeId]["sellerSpend"] += $sellerSpend;        $arrResult['adx_report_sell']["summary_day"][$date][$sellerId]["sellerSpend"] += $sellerSpend;        $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["sellerSpend"] += $sellerSpend;        $arrResult['adx_report_buy']["summary_day"][$date][$buyerId]["sellerPlay"]++;        $arrResult['adx_report_buy']["summary_day"][$date][$buyerId]["sellerSpend"] += $sellerSpend;        $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["sellerPlay"]++;        $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["sellerPlay"]++;        if($sellerId != 100144 && $sellerId != 100149) {            $arrResult['adx_report']["place_hour"][$date][$hour][$placeId][$buyerId]["sellerPlay"]++;        }        $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["sellerSpend"] += $sellerSpend;        $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["sellerSpend"] += $sellerSpend;        if($sellerId != 100144 && $sellerId != 100149) {            $arrResult['adx_report']["place_hour"][$date][$hour][$placeId][$buyerId]["sellerSpend"] += $sellerSpend;        }    }    return 5000;}//  6 点击日志(监测server记录)function parseClickLog(&$arrRow){    global $arrResult, $arrPlace;    $monitor = json_decode($arrRow[4], true);    if ($monitor == false || empty($monitor)) {        return 6001;    }    $w = intval($monitor['w']);    $h = intval($monitor['h']);    $placeId    = intval($monitor['placeId']);  //广告位ID    $time	    = intval($monitor['ctime']);    //时间戳    $buyerId    = intval($monitor['buyerId']);    $crid	    = intval($monitor['crid']);    if(date("Y", $time) < 2017){        return 6002;    }    if($placeId < 1){        return 6003;    }    if(!isset($arrPlace[$placeId])){        return 6004;    }    if($buyerId < 1){        return 6006;    }    if($crid < 1){        return 6005;    }    $mediaId = intval($arrPlace[$placeId]['mediaId']);    $sellerId = intval($arrPlace[$placeId]['sellerId']);    $date = date("Y-m-d", $time);    $hour = date("h", $time);    //卖方    $arrResult['adx_report_sell']["media_day"][$date][$mediaId]["click"] ++;    $arrResult['adx_report_sell']["place_day"][$date][$placeId]["click"] ++;    $arrResult['adx_report_sell']["summary_day"][$date][$sellerId]["click"] ++;    $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["click"] ++;    $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]['w'] = $w;    $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["h"] = $h;    //买方    $arrResult['adx_report_buy']["summary_day"][$date][$buyerId]["click"]++;    //运营库（report）    $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["click"]++;    if($sellerId != 100144 && $sellerId != 100149) {        $arrResult['adx_report']["place_hour"][$date][$hour][$placeId][$buyerId]["click"]++;    }    $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["click"]++;    //  媒体扣量处理    if(isset($monitor['sellerStatFlag']))        $sellerStatFlag	= boolval($monitor['sellerStatFlag']);    else        $sellerStatFlag = true;    if($sellerStatFlag == true){        //卖方        $arrResult['adx_report_sell']["media_day"][$date][$mediaId]["sellerClick"] ++;        $arrResult['adx_report_sell']["place_day"][$date][$placeId]["sellerClick"] ++;        $arrResult['adx_report_sell']["summary_day"][$date][$sellerId]["sellerClick"] ++;        $arrResult['adx_report_sell']["size_day"][$date][$sellerId][$w][$h]["sellerClick"] ++;        //买方        $arrResult['adx_report_buy']["summary_day"][$date][$buyerId]["sellerClick"]++;        //运营库（report）        $arrResult['adx_report']["place_day"][$date][$placeId][$buyerId]["sellerClick"]++;        if($sellerId != 100144 && $sellerId != 100149) {            $arrResult['adx_report']["place_hour"][$date][$hour][$placeId][$buyerId]["sellerClick"]++;        }        $arrResult['adx_report']["summary_day"][$date][$sellerId][$buyerId]["sellerClick"]++;    }    return 6000;}