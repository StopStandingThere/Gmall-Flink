package com.szl.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.szl.gmall.realtime.bean.TableProcess;
import com.szl.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //创建Phoenix连接
    @Override
    public void open(Configuration parameters) throws Exception {
//        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //数据类型为:
    //value:{"database":"gmall_flink_realtime","tableName":"table_process","before":{},"after":{"sourceTable":"",...},"type":"insert"}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //1.解析数据为JavaBean
        JSONObject jsonObject = JSON.parseObject(value);

        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            createTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkExtend());
        }


        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //设置key
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    //sql: create table if not exists db.tableName (id varchar primary key,name varchar)...
    private void createTable(String sinkTable, String sinkPk, String sinkColumns, String sinkExtend) {

        //如果主键为空,将"id"设置为默认主键
        if (sinkPk == null || sinkPk.equals("")) {
            sinkPk = "id";
        }
        //如果扩展为空,给空字符串
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //拼接建表SQL语句
        StringBuilder createSQL = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        //切分字段
        String[] columns = sinkColumns.split(",");

        //遍历字段
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];

            //判断是否为主键
            if (sinkPk.equals(column)) {
                createSQL.append(column).append(" varchar primary key");
            } else {
                createSQL.append(column).append(" varchar");
            }
            //判断是否为最后一个字段
            if (i < columns.length - 1) {
                //不是最后一个字段,添加","
                createSQL.append(",");
            }
        }
        createSQL.append(")").append(sinkExtend);

        //打印建表语句
        System.out.println("建表语句为: " + createSQL);

        PreparedStatement preparedStatement = null;
        try {
            //编译并执行建表语句
            preparedStatement = connection.prepareStatement(createSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建表: " + sinkTable + "失败!");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //数据类型为:
    //jsonObject:{"database":"gmall-210726-flink","tableName":"base_trademark","before":{},"after":{"id":"",...},"type":"insert"}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.提取状态信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //根据设置的key获取数据
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null){
            //2.过滤字段
            //过滤后得到的after地址值与过滤前value的地址值相同,所以后面用的value是过滤后的数据
            JSONObject after = value.getJSONObject("after");
            filterColumn(after,tableProcess.getSinkColumns());

            //3.分流
            value.put("sinkTable",tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                ctx.output(outputTag,value);
            }else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())){
                out.collect(value);
            }
        }else {
            System.out.println("组合Key: "+key+" 不存在!");
        }


    }

    //根据配置信息过滤数据
    private void filterColumn(JSONObject after, String sinkColumns) {

        String[] columns = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = after.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if (!columnsList.contains(next.getKey())){
                iterator.remove();
            }
        }

//        Set<Map.Entry<String, Object>> entries = after.entrySet();
//        entries.removeIf(next -> !columnsList.contains(next.getKey()));

    }
}
