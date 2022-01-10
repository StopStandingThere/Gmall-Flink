package com.szl.gmallpublisher.mapper;


import org.apache.ibatis.annotations.Select;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ProductStatsMapper {

    //获取商品交易额
    @Select("select sum(order_amount) from product_stats where toYYYYMMDD(stt)=${date}")
    BigDecimal selectGmv(int date);

    //获取各品牌交易额
    @Select("select tm_name,sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt)=${date} group by tm_name order by order_amount desc limit ${limit}")
    List<Map> selectGmvByTm(@RequestParam("date") int date,
                            @RequestParam("limit") int limit);
}
