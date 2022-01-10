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

public class WaterMarkTest2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Sensor> sensorSingleOutputStreamOperator = dataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new Sensor(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        SingleOutputStreamOperator<Sensor> sensorSingleOutputStreamOperator1 = sensorSingleOutputStreamOperator.assignTimestampsAndWatermarks(WatermarkStrategy.<Sensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Sensor>() {
            @Override
            public long extractTimestamp(Sensor element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        KeyedStream<Sensor, String> keyedStream = sensorSingleOutputStreamOperator1.keyBy(Sensor::getId);

        WindowedStream<Sensor, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        windowedStream.sum("vc").print();

        env.execute();
    }

}
