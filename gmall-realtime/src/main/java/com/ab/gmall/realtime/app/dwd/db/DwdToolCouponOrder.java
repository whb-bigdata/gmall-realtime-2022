package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.bean.CouponUseOrderBean;
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

public class DwdToolCouponOrder {


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

        //todo 读取优惠卷领用数据，封装成流
        Table couponUseOrder = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id, " +
                "data['using_time'] using_time, " +
                "`old`, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' ");
        DataStream<CouponUseOrderBean> couponUseOrderDS = tableEnv.toAppendStream(couponUseOrder, CouponUseOrderBean.class);

        //todo  过滤满足条件的优惠券下单数据，封装为表
        SingleOutputStreamOperator<CouponUseOrderBean> filteredDS = couponUseOrderDS.filter(
                couponUseOrderBean -> {
                    String old = couponUseOrderBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("using_time");
                    }
                    return false;
                }
        );
        //转化为表，并创建临时视图
        Table resultTable = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("result_table", resultTable);
        // todo  建立 Upsert-Kafka dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_order( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "order_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_order"));

        //todo 将数据写入upsert-kafka表
        tableEnv.executeSql("" +
                        "insert into dwd_tool_coupon_order select " +
                        "id, " +
                        "coupon_id, " +
                        "user_id, " +
                        "order_id, " +
                        "date_id, " +
                        "using_time order_time, " +
                        "ts from result_table")
                .print();

        env.execute("DwdToolCouponOrder");

    }
}
