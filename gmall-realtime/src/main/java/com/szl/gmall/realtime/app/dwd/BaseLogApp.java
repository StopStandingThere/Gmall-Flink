package com.szl.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.szl.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//TODO 数据流 web/APP -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkAPP -> Kafka(DWD)

//TODO 程序流 Mock -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)

public class BaseLogApp {
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
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/ck/dwd_log"));*/

        //2.6设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 2.读取Kafka主题 ods_base_log 数据 创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app"));

        //TODO 3.将数据转换为JSON对象并过滤掉不是JSON格式的数据至侧输出流

        OutputTag<String> outputTag = new OutputTag<String>("DirtyData") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        });

        //提出侧输出流数据并打印
        jsonObjDS.getSideOutput(outputTag).print("Dirty>>>>>>");

        //TODO 4.新老用户校验 状态编程

        //按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            //定义一个状态,用来保存is_new状态信息
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {

                //初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", Types.STRING));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //取出is_new标签数据
                String isNew = value.getJSONObject("common").getString("is_new");

                //判断是否为"1"
                if ("1".equals(isNew)) {

                    //取出状态数据
                    String state = valueState.value();

                    //判断状态是否为null
                    if (state == null) {
                        //更新状态
                        valueState.update("0");
                    } else {
                        //修改标记,将is_new修改为"0"
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }

                return value;
            }
        });

        //TODO 5.分流  页面日志->主流  启动日志->侧输出流  曝光日志->侧输出流

        OutputTag<String> startTag = new OutputTag<String>("start") {
        };

        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //尝试获取启动日志start数据
                String start = value.getString("start");

                //判断stat是否为null
                if (start != null) {
                    //启动日志
                    ctx.output(startTag, value.toJSONString());
                } else {

                    //页面日志
                    out.collect(value.toJSONString());

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");

                    //判断曝光数据是否为空
                    if (displays != null && displays.size() > 0) {

                        //获取页面id
                        String pageId = value.getJSONObject("page").getString("page_id");

                        //遍历写出 曝光数据
                        for (int i = 0; i < displays.size(); i++) {

                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);

                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 6.提取侧输出流数据 并将数据写入对应Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(startTag);

        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("Page>>>>>>");
        startDS.print("Start>>>>>>");
        displayDS.print("Display>>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));

        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));

        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 7.执行任务
        env.execute("BaseLogApp");

    }
}
