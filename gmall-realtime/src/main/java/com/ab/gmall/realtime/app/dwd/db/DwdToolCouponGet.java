package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdToolCouponGet {
    public static void main(String[] args) throws Exception {
        //todo 创建环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 从kafka中读取业务数据
        tableEnv.executeSql("create table `topic_db`( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_get"));
        // todo  读取优惠券领用数据，封装为表
        Table resultTable = tableEnv.sqlQuery("select " +
                "data['id'], " +
                "data['coupon_id'], " +
                "data['user_id'], " +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id, " +
                "data['get_time'], " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("result_table", resultTable);

        // todo  建立 Upsert-Kafka dwd_tool_coupon_get 表
        tableEnv.executeSql("create table dwd_tool_coupon_get ( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "date_id string, " +
                "get_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_get"));

        // todo  将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_tool_coupon_get select * from result_table")
                .print();

        env.execute("DwdToolCouponGet");
    }
}
