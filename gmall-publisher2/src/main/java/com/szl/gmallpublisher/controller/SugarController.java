package com.szl.gmallpublisher.controller;


import com.szl.gmallpublisher.service.ProductStatsService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date",defaultValue = "0") int date){

        if (date == 0){
            date = getToday();
        }

        BigDecimal gmv = productStatsService.getGmv(date);

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": " + gmv +
                "}";

    }
    @RequestMapping("/tm")
    public String getGmvByTm(@RequestParam(value = "date",defaultValue = "0") int date,
                             @RequestParam(value = "limit",defaultValue = "5") int limit){

        if (date == 0){
            date = getToday();
        }
        //查询ClickHouse获取数据
        Map gmvByTm = productStatsService.getGmvByTm(date, limit);

        //获取品牌名称以及对应的销售额
        Set tmName = gmvByTm.keySet();
        Collection orderAmount = gmvByTm.values();

        //封装json数据并返回

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [\"" +
                StringUtils.join(tmName, "\",\"") +
                "\"], " +
                "    \"series\": [" +
                "      { " +
                "        \"name\": \"GMV\", " +
                "        \"data\": [" +
                StringUtils.join(orderAmount, ",") +
                "] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";

    }


    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long ts = System.currentTimeMillis();
        String todayDate = sdf.format(ts);
        return Integer.parseInt(todayDate);
    }
}
