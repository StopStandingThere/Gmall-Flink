package com.szl.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimInfoJoinFunction<T> {

     String getKey(T t);

     void join(T t, JSONObject dimInfo) throws ParseException;
}
