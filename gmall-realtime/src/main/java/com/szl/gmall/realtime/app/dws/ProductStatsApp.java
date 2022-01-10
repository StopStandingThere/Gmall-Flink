package com.szl.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.szl.gmall.realtime.app.func.DimAsyncFunction;
import com.szl.gmall.realtime.bean.OrderWide;
import com.szl.gmall.realtime.bean.PaymentWide;
import com.szl.gmall.realtime.bean.ProductStats;
import com.szl.gmall.realtime.common.GmallConstant;
import com.szl.gmall.realtime.utils.ClickHouseUtil;
import com.szl.gmall.realtime.utils.DateTimeUtil;
import com.szl.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
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

        //TODO 2.消费Kafka主题数据创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 3.统一数据格式
        //3.1处理点击数据和曝光数据
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {

                //提取时间戳
                JSONObject jsonObject = JSON.parseObject(value);
                Long ts = jsonObject.getLong("ts");

                //点击数据
                JSONObject page = jsonObject.getJSONObject("page");
                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }
                //曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    //遍历曝光数据
                    for (int i = 0; i < displays.size(); i++) {

                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        //3.2处理收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        //3.3处理加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.4 处理下单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getSplit_total_amount())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        //3.5 处理支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = paymentWideDStream.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .paidOrderIdSet(orderIds)
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        //3.6处理退单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //3.7处理评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            String appraise = jsonObject.getString("appraise");
            Long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //TODO 4.合并多个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        //TODO 5.提取时间戳生成Watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 6.分组、开窗、聚合
        SingleOutputStreamOperator<ProductStats> productStatsDS = productStatsWithWMDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                        value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                        value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                        value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                        value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                        value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                        value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());
                        value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());

                        value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));
                        value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));
                        value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));

                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                        value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                        return value1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        //提取数据
                        ProductStats productStats = input.iterator().next();

                        //补充订单次数
                        productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                        productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                        productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                        //补充窗口信息
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                        //输出数据
                        out.collect(productStats);
                    }
                });

        //TODO 7.关联维度信息
        //7.1关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(productStatsDS, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getSku_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {

                productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                productStats.setTm_id(dimInfo.getLong("TM_ID"));
            }
        }, 100, TimeUnit.SECONDS);

        //7.2关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS, new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getSpu_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                productStats.setSpu_name(dimInfo.getString("SPU_NAME"));
            }
        }, 100, TimeUnit.SECONDS);

        //7.3关联品类category3维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithSpuDS, new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getCategory3_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                productStats.setCategory3_name(dimInfo.getString("NAME"));
            }
        }, 100, TimeUnit.SECONDS);

        //7.4关联品牌trademark维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithCategory3DS, new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getTm_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                productStats.setTm_name(dimInfo.getString("TM_NAME"));
            }
        }, 100, TimeUnit.SECONDS);

        //TODO 8.将数据写入ClickHouse
        productStatsWithTmDS.print(">>>>>>");

        productStatsWithTmDS.addSink(ClickHouseUtil.getSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("ProductStatsApp");

    }
}
