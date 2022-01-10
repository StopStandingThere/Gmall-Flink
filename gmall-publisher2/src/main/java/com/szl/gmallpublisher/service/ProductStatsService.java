package com.szl.gmallpublisher.service;

import java.math.BigDecimal;
import java.util.Map;

public interface ProductStatsService {

    //获取某一天的总交易额
    BigDecimal getGmv(int date);

    //获取各品牌交易额
    Map getGmvByTm(int date, int limit);

}
