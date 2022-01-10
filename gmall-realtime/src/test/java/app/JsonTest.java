package app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class JsonTest {
    public static void main(String[] args) {
        String jsonStr  = "{\"sinkTable\":\"dwd_refund_payment\",\"database\":\"gmall_flink\",\"before\":{},\"after\":{\"callback_time\":\"2022-01-01 19:43:09\",\"payment_type\":\"1101\",\"out_trade_no\":\"657798882788679\",\"refund_status\":\"0702\",\"create_time\":\"2022-01-01 19:43:09\",\"total_amount\":2598.00,\"subject\":\"退款\",\"sku_id\":6,\"id\":15,\"order_id\":27081},\"type\":\"insert\",\"tableName\":\"refund_payment\"}\n";

        JSONObject jsonObject = JSON.parseObject(jsonStr);
        System.out.println(jsonObject.getJSONObject("after").getLong("order_id"));
        System.out.println(jsonObject.getString("create_time"));
    }
}
