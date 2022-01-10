package com.szl.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.szl.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String table, String key) throws Exception {

        //查询Redis
        Jedis jedis = RedisUtil.getJedis();
        //定义RedisKey
        String redisKey = "DIM:" + table + ":" + key;
        String jsonStr = jedis.get(redisKey);

        //判断Redis中是否有缓存过的数据
        if (jsonStr != null) {
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return JSON.parseObject(jsonStr);
        }

        //拼接sql
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA +
                "." + table + " where id = '" + key + "'";
        System.out.println("查询SQL为: " + sql);

        //查询数据
        List<JSONObject> list = JdbcUtil.queryData(connection, sql, JSONObject.class, false);

        //写入Redis
        JSONObject dimInfo = list.get(0);
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();
        //返回结果
        return dimInfo;
    }

    public static void delDimInfo(String table, String key){

        //定义RedisKey
        String redisKey = "DIM:" + table + ":" + key;

        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);

        jedis.close();
    }

    //测试
    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();

        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "12"));

        long end = System.currentTimeMillis();

        System.out.println(end-start);

       /* System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "13"));
        long end1 = System.currentTimeMillis();
        System.out.println(end1-end);*/

        connection.close();
    }
}
