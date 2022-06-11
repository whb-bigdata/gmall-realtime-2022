package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.bean.CouponUsePayBean;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

public class DwdToolCouponPay {
    public static void main(String[] args) throws Exception {
//todo 环境准备
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 从kafka中读取业务数据，封装成FlinkSql
        tableEnv.executeSql("create table `topic_db` ( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`old` string, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_order"));
        // TODO 4. 读取优惠券领用表数据，封装为流，区别order的using_time此处为used_time
        Table couponUsePay = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['used_time'],'yyyy-MM-dd') date_id, " +
                "data['used_time'] used_time, " +
                "`old`, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' ");
        DataStream<CouponUsePayBean> couponUsePayDS = tableEnv.toAppendStream(couponUsePay, CouponUsePayBean.class);

        // TODO 5. 过滤满足条件的优惠券下单数据，封装为表
        SingleOutputStreamOperator<CouponUsePayBean> filteredDS = couponUsePayDS.filter(
                couponUsePayBean -> {
                    String old = couponUsePayBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("used_time");
                    }
                    return false;
                }
        );
        Table resultTable = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 6. 建立 Upsert-Kafka dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_pay( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "payment_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_pay"));

        // TODO 7. 将数据写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                        "insert into dwd_tool_coupon_pay select " +
                        "id, " +
                        "coupon_id, " +
                        "user_id, " +
                        "order_id, " +
                        "date_id, " +
                        "used_time payment_time, " +
                        "ts from result_table")
                .print();

        env.execute("DwdToolCouponPay");
    }
}
