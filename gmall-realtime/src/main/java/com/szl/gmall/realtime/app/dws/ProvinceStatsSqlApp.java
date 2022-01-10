package com.szl.gmall.realtime.app.dws;

import com.szl.gmall.realtime.bean.ProvinceStats;
import com.szl.gmall.realtime.utils.ClickHouseUtil;
import com.szl.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /*   //2.Flink-CDC 将读取binlog的位置信息,以状态的方式保存在Checkpoint,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序

        //2.1开启Checkpoint每隔5秒做一次CK
        env.enableCheckpointing(5000L);

        //2.2指定Checkpoint的一致性语义\
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //2.3设置任务关闭后保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //2.4指定从CK自动重启策略,老版本重试次数较多可设置,新版本重试次数较少,可设置也可以不设置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));

        //2.5设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/dwd_db"));*/

        //2.6设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 2.使用DDL的方式创建动态表,提取事件时间生成watermark
        String groupId = "province_stats_sql_app";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("create table order_wide( " +
                "  province_id bigint, " +
                "  province_name string, " +
                "  province_area_code string, " +
                "  province_iso_code string, " +
                "  province_3166_2_code string, " +
                "  order_id bigint, " +
                "  split_total_amount decimal, " +
                "  create_time string, " +
                "  rt as TO_TIMESTAMP(create_time), " +
                "  watermark for rt as rt - interval '2' second " +
                ")with("+ MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId) +")");

        //TODO 3.计算订单数 订单总金额  开窗10秒的滚动窗口
        Table resultTable = tableEnv.sqlQuery("select " +
                "  date_format(TUMBLE_START(rt, interval '10' second),'yyyy-MM-dd HH:mm:ss') stt, " +
                "  date_format(TUMBLE_END(rt, interval '10' second),'yyyy-MM-dd HH:mm:ss') edt, " +
                "  province_id, " +
                "  province_name, " +
                "  province_area_code, " +
                "  province_iso_code, " +
                "  province_3166_2_code, " +
                "  count(distinct order_id) order_count, " +
                "  sum(split_total_amount) order_amount, " +
                "  UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide " +
                "group by " +
                "  province_id, " +
                "  province_name, " +
                "  province_area_code, " +
                "  province_iso_code, " +
                "  province_3166_2_code, " +
                "  TUMBLE(rt, interval '10' second)");

        //TODO 4.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(resultTable, ProvinceStats.class);

        //TODO 5.将数据写入到ClickHouse
        provinceStatsDataStream.print(">>>>>>");

        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动任务
        env.execute("ProvinceStatsSqlApp");
    }
}
