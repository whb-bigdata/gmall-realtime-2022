package com.ab.gmall.realtime.app.dwd.db;

import com.ab.gmall.realtime.bean.OrderInfoRefundBean;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.ab.gmall.realtime.util.MysqlUtil;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

public class DwdTradeOrderRefund {
    public static void main(String[] args) throws Exception {

        //todo 创建环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo  Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` string, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_refund"));
        //todo 读取退单表数据
        Table orderRefundInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['refund_type'] refund_type, " +
                "data['refund_num'] refund_num, " +
                "data['refund_amount'] refund_amount, " +
                "data['refund_reason_type'] refund_reason_type, " +
                "data['refund_reason_txt'] refund_reason_txt, " +
                "data['create_time'] create_time, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'order_refund_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        //todo 读取订单表数据并转化为流
      Table orderInfo = tableEnv.sqlQuery("select " +
              "data['id'] id, " +
              "data['province_id'] province_id, " +
              "`old` " +
              "from topic_db " +
              "where `table` = 'order_info' " +
              "and `type` = 'update' " +
              "and data['order_status']='1005'");
      DataStream<OrderInfoRefundBean> orderInfoRefundDS = tableEnv.toAppendStream(orderInfo, OrderInfoRefundBean.class);

      //todo 过滤符合条件的订单表退单数据
        SingleOutputStreamOperator<OrderInfoRefundBean> filteredDS = orderInfoRefundDS.filter(
                orderInfoRefund -> {
                    String old = orderInfoRefund.getOld();
                    //old 不为null说明有old字段
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        //order_status要有一个key为order_status，而不管里面的值为多少
                        return changeKeys.contains("order_status");
                    }
                    return false;
                }
        );
        //todo 将流再转回表
        Table orderInfoRefund = tableEnv.fromDataStream(filteredDS);
        tableEnv.createTemporaryView("order_info_refund",orderInfoRefund);

        //todo 创建mysql-lookup字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //todo 关联三张表获得退单宽表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "ri.id, " +
                "ri.user_id, " +
                "ri.order_id, " +
                "ri.sku_id, " +
                "oi.province_id, " +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id, " +
                "ri.create_time, " +
                "ri.refund_type, " +
                "type_dic.dic_name, " +
                "ri.refund_reason_type, " +
                "reason_dic.dic_name, " +
                "ri.refund_reason_txt, " +
                "ri.refund_num, " +
                "ri.refund_amount, " +
                "ri.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_refund_info ri " +
                "left join  " +
                "order_info_refund oi " +
                "on ri.order_id = oi.id " +
                "left join  " +
                "base_dic for system_time as of ri.proc_time as type_dic " +
                "on ri.refund_type = type_dic.dic_code " +
                "left join " +
                "base_dic for system_time as of ri.proc_time as reason_dic " +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
        //todo 建立 Upsert-Kafka dwd_trade_order_refund 表
        tableEnv.executeSql("create table dwd_trade_order_refund( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "date_id string, " +
                "create_time string, " +
                "refund_type_code string, " +
                "refund_type_name string, " +
                "refund_reason_type_code string, " +
                "refund_reason_type_name string, " +
                "refund_reason_txt string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_refund"));

        // TODO 11. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_trade_order_refund select * from result_table");

        env.execute("DwdTradeOrderRefund");


    }
}
