package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.ab.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionComment {
    public static void main(String[] args) throws Exception {
        //todo 环境准备
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // todo  从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_comment"));

        // todo  读取评论表数据
        Table commentInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "data['order_id'] order_id, " +
                "data['create_time'] create_time, " +
                "data['appraise'] appraise, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'comment_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // todo 5. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // todo 6. 关联两张表
        Table resultTable = tableEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id, " +
                "ci.sku_id, " +
                "ci.order_id, " +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id, " +
                "ci.create_time, " +
                "ci.appraise, " +
                "dic.dic_name, " +
                "ts " +
                "from comment_info ci " +
                "left join " +
                "base_dic for system_time as of ci.proc_time as dic " +
                "on ci.appraise = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // todo  建立 Upsert-Kafka dwd_interaction_comment 表
        tableEnv.executeSql("create table dwd_interaction_comment( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "order_id string, " +
                "date_id string, " +
                "create_time string, " +
                "appraise_code string, " +
                "appraise_name string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_interaction_comment"));

        // todo  将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                        "insert into dwd_interaction_comment select * from result_table")
                .print();

        env.execute("DwdInteractionComment");
    }


}
