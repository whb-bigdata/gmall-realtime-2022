package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionFavorAdd {
    public static void main(String[] args) throws Exception {
        //todo 环境准备
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // todo 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_favor_add"));

        // todo  读取收藏表数据
        Table favorInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "date_format(data['create_time'],'yyyy-MM-dd') date_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'favor_info' " +
                "and `type` = 'insert' " +
                "or (`type` = 'update' and `old`['is_cancel'] = '1' and data['is_cancel'] = '0')");
        tableEnv.createTemporaryView("favor_info", favorInfo);

        // todo  创建 Upsert-Kafka dwd_interaction_favor_add 表
        tableEnv.executeSql("create table dwd_interaction_favor_add ( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "date_id string, " +
                "create_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_interaction_favor_add"));

        // todo  将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                        "insert into dwd_interaction_favor_add select * from favor_info")
                .print();

        env.execute("DwdInteractionFavorAdd");

    }
    
}
