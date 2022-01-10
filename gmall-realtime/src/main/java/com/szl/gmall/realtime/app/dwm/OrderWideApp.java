package com.szl.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.szl.gmall.realtime.app.func.DimAsyncFunction;
import com.szl.gmall.realtime.bean.OrderDetail;
import com.szl.gmall.realtime.bean.OrderInfo;
import com.szl.gmall.realtime.bean.OrderWide;
import com.szl.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
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

        //TODO 2.读取Kafka订单主题dwd_order_info和订单明细主题dwd_order_detail数据 创建数据流
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_app";

        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));

        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean,并提取时间戳生成watermark

        //OrderInfo
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

            //补全时间字段
            String createTime = orderInfo.getCreate_time();
            String[] dateTime = createTime.split(" ");
            orderInfo.setCreate_date(dateTime[0]);
            orderInfo.setCreate_hour(dateTime[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(createTime).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //OrderDetail
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);

            //补全时间字段
            String createTime = orderDetail.getCreate_time();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(createTime).getTime());

            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //TODO 4.双流Join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
        orderWideDS.print("OrderWide>>>>>>");

        //TODO 5.关联维度信息
        /*orderWideDS.map(new RichMapFunction<OrderWide, OrderWide>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public OrderWide map(OrderWide value) throws Exception {
                return null;
            }
        });*/

        //TODO 5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfoDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getUser_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                //关联用户年龄信息
                String birthday = dimInfo.getString("BIRTHDAY");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                long currentTimeMillis = System.currentTimeMillis();
                long birthdayTimeStamp = sdf.parse(birthday).getTime();

                long age = (currentTimeMillis - birthdayTimeStamp) / (1000L * 60 * 60 * 24 * 365);
                orderWide.setUser_age((int) age);

                //关联用户性别信息
                orderWide.setUser_gender(dimInfo.getString("GENDER"));

            }
        }, 100, TimeUnit.SECONDS);

        //TODO 5.2关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceInfoDS = AsyncDataStream.unorderedWait(orderWideWithUserInfoDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                orderWide.setProvince_name(dimInfo.getString("NAME"));
                orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
            }
        }, 100, TimeUnit.SECONDS);

        orderWideWithProvinceInfoDS.print("orderWideWithProvinceInfoDS>>>>>>");

        //TODO 5.3关联商品SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuInfoDS = AsyncDataStream.unorderedWait(orderWideWithProvinceInfoDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getSku_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
            }
        }, 100, TimeUnit.SECONDS);

        //TODO 5.4关联商品SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuInfoDS = AsyncDataStream.unorderedWait(orderWideWithSkuInfoDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getSpu_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
            }
        }, 100, TimeUnit.SECONDS);

        //TODO 5.5关联品牌BaseTrademark维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTrademarkInfoDS = AsyncDataStream.unorderedWait(orderWideWithSpuInfoDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getTm_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                orderWide.setTm_name(dimInfo.getString("TM_NAME"));
            }
        }, 100, TimeUnit.SECONDS);

        //TODO 5.6关联分类Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3InfoDS = AsyncDataStream.unorderedWait(orderWideWithTrademarkInfoDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getCategory3_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {

                orderWide.setCategory3_name(dimInfo.getString("NAME"));
            }
        }, 100, TimeUnit.SECONDS);

        //TODO 6.将数据写入Kafka主题dwm_order_wide
        orderWideWithCategory3InfoDS.print("orderWideWithCategory3InfoDS>>>>>>");

        orderWideWithCategory3InfoDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        //TODO 7.启动任务
        env.execute("OrderWideApp");
    }
}
