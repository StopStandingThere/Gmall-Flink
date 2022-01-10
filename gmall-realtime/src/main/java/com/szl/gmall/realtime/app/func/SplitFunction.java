package com.szl.gmall.realtime.app.func;

import com.szl.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String keyword){

        List<String> list = null;
        try {
            list = KeywordUtil.splitKeyword(keyword);

            for (String word : list) {
                collect(Row.of(word));
            }

        } catch (IOException e) {
            collect(Row.of(keyword));
        }

    }
}
