package app;

import bean.Sensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import trigger.MyTrigger;

public class TestTrigger {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[1]);
            }
        }));

        SingleOutputStreamOperator<Sensor> sensorSingleOutputStreamOperator = stringSingleOutputStreamOperator.map(line -> {
            String[] fields = line.split(",");
            return new Sensor(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        KeyedStream<Sensor, String> keyedStream = sensorSingleOutputStreamOperator.keyBy(Sensor::getId);

        WindowedStream<Sensor, String, TimeWindow> windowedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new MyTrigger());

        windowedStream.sum("vc").print();

        env.execute();
    }


}
