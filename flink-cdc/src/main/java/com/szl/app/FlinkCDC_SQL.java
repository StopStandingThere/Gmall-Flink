package com.szl.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.创建表并连接MySQL
        tableEnv.executeSql("create table cdc_binlog(" +
                "tm_id string," +
                "tm_name string)" +
                "with(" +
                "'hostname' = 'hadoop102'," +
                "'connector' = 'mysql-cdc'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = 'root'," +
                "'database-name' = 'gmall_realtime'," +
                "'table-name' = 'base_trademark'," +
                "'scan.incremental.snapshot.enabled' = 'false')");

        tableEnv.executeSql("select * from cdc_binlog").print();
    }
}
