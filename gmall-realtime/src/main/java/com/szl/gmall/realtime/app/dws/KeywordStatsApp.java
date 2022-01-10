package com.szl.gmall.realtime.app.dws;

import com.szl.gmall.realtime.app.func.SplitFunction;
import com.szl.gmall.realtime.bean.KeywordStats;
import com.szl.gmall.realtime.utils.ClickHouseUtil;
import com.szl.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
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

        //TODO 2.使用DDL创建动态表 提取事件时间生成Watermark
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";

        tableEnv.executeSql("create table page_log( " +
                "  page map<string,string>, " +
                "  ts bigint, " +
                "  rt as TO_TIMESTAMP_LTZ(ts, 3), " +
                "  watermark for rt as rt - interval '2' second " +
                "  )with("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId) +")");

        //TODO 3.过滤数据 只需要搜索的数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "  page['item'] keywords, " +
                "  rt " +
                "from page_log " +
                "where page['last_page_id'] = 'search' " +
                "and page['item'] is not null");

        tableEnv.createTemporaryView("filter_table",filterTable);

        //TODO 4.注册UDTF函数 并使用完成切词
        tableEnv.createTemporarySystemFunction("split_function",SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("" +
                "select " +
                "  word, " +
                "  rt " +
                "from filter_table,lateral table(split_function(keywords))");

        tableEnv.createTemporaryView("split_table",splitTable);

        //TODO 5.计算每个分词出现的次数
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "  'search' source, " +
                "  date_format(TUMBLE_START(rt, interval '10' second),'yyyy-MM-dd HH:mm:ss') stt, " +
                "  date_format(TUMBLE_END(rt, interval '10' second),'yyyy-MM-dd HH:mm:ss') edt, " +
                "  word keyword, " +
                "  count(*) ct, " +
                "  UNIX_TIMESTAMP()*1000 ts " +
                "from split_table " +
                "group by " +
                "  word, " +
                "  TUMBLE(rt, interval '10' second)");

        //TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //TODO 7.将数据写入ClickHouse
        keywordStatsDataStream.print(">>>>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("KeywordStatsApp");
    }
}
