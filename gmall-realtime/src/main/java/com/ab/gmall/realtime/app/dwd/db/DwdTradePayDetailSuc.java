package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.ab.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradePayDetailSuc {

    public static void main(String[] args) throws Exception {
        //todo 创建环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 读取订单明细表业务数据，封装为 Flink SQL 表
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
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_pay_detail"));
                //todo 读取支付表数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_pay_detail"));
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "`old`, " +
                "pt " +
                "from topic_db " +
                "where `table` = 'payment_info' " +
                "and `type` = 'update' " +
                "and data['payment_status']='1602' " +
                "and `old`['payment_status'] is not null");
        tableEnv.createTemporaryView("payment_info", paymentInfo);
        //todo 读取mysql 中的base_dic表构建维度
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //todo 关联3张表获得支付成功宽表，其中一张表是look up join
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.order_detail_id, " +
                "    od.order_id, " +
                "    od.user_id, " +
                "    od.sku_id, " +
                "    od.province_id, " +
                "    od.activity_id, " +
                "    od.activity_rule_id, " +
                "    od.coupon_id, " +
                "    pi.payment_type payment_type_code, " +
                "    dic.dic_name payment_type_name, " +
                "    pi.callback_time, " +
                "    od.source_id, " +
                "    od.source_type, " +
                "    od.sku_num, " +
                "    od.split_original_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    od.split_total_amount split_payment_amount, " +
                "    pi.pt " +
                "from payment_info pi " +
                "join dwd_trade_order_detail_table od " +
                "on pi.order_id = od.order_id " +
                "join base_dic FOR SYSTEM_TIME AS OF pi.pt dic " +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //todo 创建 Kafka dwd_trade_pay_detail 表
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc( " +
                "order_detail_id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "callback_time string, " +
                "source_id string, " +
                "source_type string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_payment_amount string, " +
                "pt TIMESTAMP_LTZ(3) " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_pay_detail_suc", ""));

        //todo  将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table")
                .print();

        env.execute();



    }
}

