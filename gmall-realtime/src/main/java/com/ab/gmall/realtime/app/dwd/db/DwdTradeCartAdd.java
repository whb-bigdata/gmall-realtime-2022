package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.ab.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //env.setStateBackend(new HashMapStateBackend());
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");

        //设置状态存储时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //todo 使用ddl方式读取kafka topic maxwell
//        tableEnv.executeSql("create table maxwell(" +
//                "  `database` String, " +
//                "  `table` String, " +
//                "  `type` String, " +
//                "  `data` Map<String,String>, " +
//                "  `old` Map<String,String>, " +
//                "  `pt` AS PROCTIME() " +
//                ")" +
//                "WITH ( " +
//                "  'connector' = 'kafka', " +
//                "  'topic' = '" + "maxwell" + "', " +
//                "  'properties.bootstrap.servers' = '" + "hadoop100:9092,hadoop101:9092,hadoop102:9092" + "', " +
//                "  'format' = 'json' " +
//                ")");
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_cart_add"));
        //打印测试
//        Table table = tableEnv.sqlQuery("select * from maxwell");
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print(">>>>>>>>>>>>>>>>");
        //todo 过滤加购数据
        Table carAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['user_id'] user_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['cart_price'] cart_price, " +
                "    if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) sku_num, " +
                "    data['sku_name'] sku_name, " +
                "    data['is_checked'] is_checked, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['is_ordered'] is_ordered, " +
                "    data['order_time'] order_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='cart_info' " +
                "and (`type`='insert'  " +
                "    or (`type`='update'  " +
                "        and `old`['sku_num'] is not null " +
                "        and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");

        tableEnv.createTemporaryView("cart_add",carAddTable);
//        tableEnv.sqlQuery("select * from cart_add").execute().print();
        //todo 读取mysql中的base_dic构建唯表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //todo 关联两张表，唯独退化
        Table resultTable =  tableEnv.sqlQuery("" +
                "select " +
                "    c.id, " +
                "    c.user_id, " +
                "    c.sku_id, " +
                "    c.cart_price, " +
                "    c.sku_num, " +
                "    c.sku_name, " +
                "    c.is_checked, " +
                "    c.create_time, " +
                "    c.operate_time, " +
                "    c.is_ordered, " +
                "    c.order_time, " +
                "    c.source_type, " +
                "    c.source_id, " +
                "    b.dic_name " +
                "from cart_add c " +
                "join base_dic FOR SYSTEM_TIME AS OF c.pt as b " +
                "on c.source_type = b.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
//        tableEnv.sqlQuery("select * from result_table").execute().print();
        //todo 数据写回kafka dwd层
        String sinkTopic = "dwd_trade_cart_add";
        tableEnv.executeSql("" +
                "create table trade_cart_add( " +
                "    id string, " +
                "    user_id string, " +
                "    sku_id string, " +
                "    cart_price string, " +
                "    sku_num int, " +
                "    sku_name string, " +
                "    is_checked string, " +
                "    create_time string, " +
                "    operate_time string, " +
                "    is_ordered string, " +
                "    order_time string, " +
                "    source_type string, " +
                "    source_id string, " +
                "    dic_name string " +
                ")" + MyKafkaUtil.getKafkaDDL(sinkTopic, ""));
        tableEnv.executeSql("insert into trade_cart_add select * from result_table").print();

        //todo 启动
        env.execute("DwdTradeCartAdd");
    }
}
