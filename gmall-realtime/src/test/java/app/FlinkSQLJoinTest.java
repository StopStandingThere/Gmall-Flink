package app;

import bean.Bean1;
import bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkSQLJoinTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        //设置状态的TTL
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<Bean1> ds1 = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                    @Override
                    public long extractTimestamp(Bean1 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        SingleOutputStreamOperator<Bean2> ds2 = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                    @Override
                    public long extractTimestamp(Bean2 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        tableEnv.createTemporaryView("t1", ds1);
        tableEnv.createTemporaryView("t2", ds2);

        //内连接      左表：OnCreateAndWrite  右表：OnCreateAndWrite
        //tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id=t2.id").print();

        //左外连接    左表：OnReadAndWrite    右表：OnCreateAndWrite
        //tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id=t2.id").print();

        //右外连接    左表：OnCreateAndWrite  右表：OnReadAndWrite
        //tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 right join t2 on t1.id=t2.id").print();

        //全外连接    左表：OnReadAndWrite    右表：OnReadAndWrite
        tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 full join t2 on t1.id=t2.id").print();

    }
}
