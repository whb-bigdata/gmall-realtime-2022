package com.ab.gmall.realtime.app.func;

import com.ab.gmall.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
//todo 输出的单列，名字为word
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyword) {

        List<String> list;
        try {
            list = KeywordUtil.splitKeyWord(keyword);
            //todo 遍历结果，多行输出
            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            //todo 不能切词，就原封不动的写出
            collect(Row.of(keyword));
        }

    }
}
