package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.app.func.OrderDetailFilterFunction;
import com.ab.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;

public class DwsTradeTrademarkCategoryUserSpuOrderWindow {
    public static void main(String[] args) {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        //todo 读取Dwd层 获取过滤后的OrderDetail表
        String groupId = "sku_user_order_window";
        //todo 过滤（""不需要，保留"insert"），转换数据为JSON
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);
        //todo 转换数据为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> skuUserOrderDS = orderDetailJsonObjDS.map(json -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(json.getString("order_id"));

            return TradeTrademarkCategoryUserSpuOrderBean.builder()
                    .skuId(json.getString("sku_id"))
                    .userId(json.getString("user_id"))
                    .orderIdSet(orderIds)
                    .orderAmount(json.getDouble("split_total_amount"))
                    .ts(DateFormatUtil.toTs(json.getString("order_create_time"), true))
                    .build();
        });


    }
}
