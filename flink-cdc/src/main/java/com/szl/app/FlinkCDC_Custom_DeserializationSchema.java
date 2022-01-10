package com.szl.app;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDC_Custom_DeserializationSchema {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.创建Flink-MySQL-CDC的source
        DebeziumSourceFunction<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .startupOptions(StartupOptions.initial())
                .databaseList("gmall_realtime")
                .tableList("gmall_realtime.base_trademark")
                .deserializer(new MyDeserializer())
                .build();

        //3.使用CDC Source从MySQL读数据
        DataStreamSource<String> streamSource = env.addSource(mysqlSource);

        //4.打印
        streamSource.print();

        //5.执行
        env.execute();
    }

    public static class MyDeserializer implements DebeziumDeserializationSchema<String>{

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

            //创建用来存放结果数据的JSONObject对象
            JSONObject result = new JSONObject();

            //获取主题信息
            String topic = sourceRecord.topic();

            String[] split = topic.split("\\.");

            //获取库名
            String datebase = split[1];

            //获取表名
            String tableName = split[2];

            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            String type = operation.toString().toLowerCase();

            if ("create".equals(type)){
                type = "insert";
            }

            //获取值信息,并转为Struct类型
            Struct value = (Struct) sourceRecord.value();

            //获取before数据
            Struct beforeStruct = value.getStruct("before");

            JSONObject beforeJson = new JSONObject();
            if (beforeStruct != null){
                List<Field> fields = beforeStruct.schema().fields();

                for (Field field : fields) {
                    beforeJson.put(field.name(),beforeStruct.get(field));
                }
            }

            //获取after数据
            Struct afterStruct = value.getStruct("after");

            JSONObject afterJson = new JSONObject();

            if (afterStruct != null){
                List<Field> fields = afterStruct.schema().fields();

                for (Field field : fields) {
                    afterJson.put(field.name(),afterStruct.get(field));
                }
            }

            //将数据封装到结果集中
            result.put("database",datebase);
            result.put("tableName",tableName);
            result.put("before",beforeJson);
            result.put("after",afterJson);
            result.put("type",type);

            //将数据发送至下游
            collector.collect(result.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
