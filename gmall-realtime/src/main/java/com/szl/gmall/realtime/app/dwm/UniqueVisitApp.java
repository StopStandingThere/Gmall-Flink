package com.szl.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.szl.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

//TODO 数据流 web/APP ->Nginx ->日志服务器 ->Kafka(ODS) ->FlinkAPP   ->Kafka(DWD) ->FlinkAPP  ->Kafka(DWM)

//TODO 程序流 Mock ->Nginx ->Logger.sh ->Kafka(ZK)  ->BaseLogApp ->Kafka(ZK) ->UniqueVisitApp ->Kafka(ZK)

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建流式数据处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        //TODO 2.从Kafka主题 dwd_page_log 读取数据,创建流
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        String groupId = "unique_visit_app";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        //TODO 3.将数据转换为JSONObject对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //TODO 5.使用状态编程对数据进行去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            //定义一个状态用来保存时间信息
            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("value-state", String.class);

                //设置状态的TTL为一天,并设置TTL重置类型
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                valueState = getRuntimeContext().getState(valueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //获取上一跳页面id,last_page_id
                JSONObject page = value.getJSONObject("page");
                String lastPageId = page.getString("last_page_id");

                //判断lastPageId是否为null
                if (lastPageId == null) {
                    //获取状态信息
                    String visitDate = valueState.value();

                    //获取当前时间信息
                    String currentDate = sdf.format(value.getLong("ts"));

                    //判断状态信息是否存在
                    if (visitDate == null || !visitDate.equals(currentDate)) {
                        valueState.update(currentDate);
                        return true;
                    } else {
                        return false;
                    }

                } else {

                    return false;
                }
            }
        });

        //TODO 6.将数据写入Kafka主题 dwm_unique_visit

        filterDS.print("filterDS>>>>>>");
        filterDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //TODO 7.启动任务
        env.execute("UniqueVisitApp");
    }
}
