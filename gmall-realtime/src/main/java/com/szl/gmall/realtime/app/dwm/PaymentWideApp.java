package com.szl.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.szl.gmall.realtime.bean.OrderWide;
import com.szl.gmall.realtime.bean.PaymentInfo;
import com.szl.gmall.realtime.bean.PaymentWide;
import com.szl.gmall.realtime.utils.DateTimeUtil;
import com.szl.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
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

        //TODO 2.读取Kafka主题dwd_payment_info和dwm_order_wide数据 创建流
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "payment_wide_app";

        DataStreamSource<String> paymentInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));

        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoKafkaDS.map(jsonStr ->
                JSON.parseObject(jsonStr, PaymentInfo.class));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));

        //TODO 4.提取时间戳生成Watermark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWMDS = paymentInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {

                        //有线程安全问题
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }

                        //调用时间解析工具类没有线程安全问题
                        //return DateTimeUtil.toTs(element.getCreate_time());
                    }
                }));

        SingleOutputStreamOperator<OrderWide> orderWideWithWMDS = orderWideDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                        //return DateTimeUtil.toTs(element.getCreate_time());
                    }
                }));

        //TODO 5.双流Join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoWithWMDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideWithWMDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        //TODO 6.将数据写入Kafka主题dwm_payment_wide

        paymentWideDS.print("paymentWideDS>>>>>>");

        paymentWideDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        //TODO 7.启动任务
        env.execute("PaymentWideApp");
    }
}
