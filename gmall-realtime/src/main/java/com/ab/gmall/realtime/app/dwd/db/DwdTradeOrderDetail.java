package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail {
    public static void main(String[] args) {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //设置状态存储事件
        tableEnv.getConfig().setIdleStateRetention(Duration.ofDays(3));

        //todo DDL方式读取 kafka中topic_db主题数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_order_detail"));

        //todo 过滤订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] order_detail_id, " +
                "    data['order_id'] order_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['sku_name'] sku_name, " +
                "    data['order_price'] order_price, " +
                "    data['sku_num'] sku_num, " +
                "    data['create_time'] create_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    cast(cast(data['sku_num'] as decimal(16,2)) * cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, " +
                "    data['split_total_amount'] split_total_amount, " +
                "    data['split_activity_amount'] split_activity_amount, " +
                "    data['split_coupon_amount'] split_coupon_amount, " +
                "    pt " +
                "from topic_db " +
                "where `database`='gmall-211027-flink' " +
                "and `table`='order_detail' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail", orderDetailTable);
        //todo 过滤订单数据

        //todo 过滤出订单明细活动数据


        //todo 订单明细购物卷数据

        //todo 构建MySQL-lookup表  base_dic


        //todo 关联5张表


        //todo 创建kafka upsert-kafka 表


    }
}
