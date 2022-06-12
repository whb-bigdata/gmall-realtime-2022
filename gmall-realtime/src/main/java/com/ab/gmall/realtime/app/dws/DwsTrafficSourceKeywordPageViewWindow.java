package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.app.func.SplitFunction;
import com.ab.gmall.realtime.bean.KeywordBean;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        //todo 创建环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 使用ddl方式读取dwd层页面浏览日志，创建表，同时获取事件事件生成watermark
        tableEnv.executeSql("" +
                "create table page_log( " +
                "    `page` map<string,string>, " +
                "    `ts` bigint, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_traffic_page_log", "Dws_Traffic_Source_Keyword_PageView_Window"));
        //todo 过滤出搜索数据
        Table keyWordTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] key_word, " +
                "    rt " +
                "from " +
                "    page_log " +
                "where page['item'] is not null " +
                "and page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("key_word_table",keyWordTable);

        //todo 使用自定义函数分词处理
            //注册函数
        tableEnv.createFunction("SplitFunction", SplitFunction.class);
            //处理数据
        Table splitTable = tableEnv.sqlQuery(""+
                "select " +
                "   word," +
                "   rt " +
                "FROM key_word_table, LATERAL TABLE(SplitFunction(key_word))" );
        tableEnv.createTemporaryView("split_table", splitTable);
        //todo 分组开窗聚合,group by TUMBLE(rt, INTERVAL '10' SECOND),word:分组+开窗
        // DATE_FORMAT将时间戳变为'yyyy-MM-dd HH:mm:ss'
        // 'search' ，用来表示这是source的类型
        // 使用系统时间当作版本时间
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP() ts " +
                "from " +
                "    split_table " +
                "group by TUMBLE(rt, INTERVAL '10' SECOND),word");

        //todo 将数据转换为流，转换过来的时候按照字段名来对应，表和类的字段名要一致
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>>>>>>");

        //todo 将数据写出到clickhouse

        //todo 启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
