package com.szl.gmall.realtime.app.ods;


import com.szl.gmall.realtime.app.func.MyCustomDeserializer;
import com.szl.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink_CDCWithCustomSchema_Ods {
    public static void main(String[] args) throws Exception {
        //1.创建流式数据处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 生产环境中设置为Kafka主题的分区个数
        env.setParallelism(1);

      /*  //2.Flink-CDC 将读取binlog的位置信息,以状态的方式保存在Checkpoint,如果要做到断点续传,需要从Checkpoint或者Savepoint重新启动程序

        //2.1开启Checkpoint 并设置Checkpoint一致性语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        //2.2设置任务关闭后保留最后一次Checkpoint信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //2.3设置状态后端
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop102:8020/flink/cdc"));

        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flink/cdc");

        //2.4指定从CK自动重启策略,老版本重试次数较多可设置,新版本重试次数较少,可设置也可以不设置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));

        //2.5设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        //TODO 3.创建Flink-MySQL-CDC 的 Source
        DebeziumSourceFunction<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall_flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyCustomDeserializer())
                .build();

        //4.使用CDC source从MySQL读取数据
        DataStreamSource<String> streamSource = env.addSource(mysqlSource);

        //5.将数据发送至Kafka
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        streamSource.print();

        env.execute();

    }
}
