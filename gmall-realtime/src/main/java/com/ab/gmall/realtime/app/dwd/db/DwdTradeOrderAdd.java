package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.util.MyEnv;

import com.ab.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderAdd {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo ddl读取dwd_trade_order_detail 主题数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail_table( " +
                "    `order_detail_id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `order_create_time` string, " +
                "    `source_type` string, " +
                "    `source_id` string, " +
                "    `split_original_amount` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `pt` TIMESTAMP_LTZ(3), " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `activity_create_time` string , " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `coupon_create_time` string , " +
                "    `dic_name` string " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_order_add"));
        //todo 过滤出下单数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    * " +
                "from dwd_trade_order_detail_table " +
                "where `type`='insert'");
        tableEnv.createTemporaryView("filter_table", filterTable);
        //todo 创建kafka下单数据表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_add_table( " +
                "    `order_detail_id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `order_create_time` string, " +
                "    `source_type` string, " +
                "    `source_id` string, " +
                "    `split_original_amount` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `pt` TIMESTAMP_LTZ(3), " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `activity_create_time` string , " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `coupon_create_time` string , " +
                "    `dic_name` string " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_add", ""));

        //todo 数据写出到kafka
        tableEnv.executeSql("insert into dwd_trade_order_add_table select * from filter_table").print();

        //todo 启动任务
        env.execute("DwdTradeOrderAdd");






    }
}
