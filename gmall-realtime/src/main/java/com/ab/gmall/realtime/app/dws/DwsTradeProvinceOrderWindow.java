package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.app.func.DimAsyncFunction;
import com.ab.gmall.realtime.app.func.OrderDetailFilterFunction;
import com.ab.gmall.realtime.bean.TradeProvinceOrderWindow;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        //todo 读取dwd层数据，并去重过滤
        String groupId = "dws_trade_province_order_window_211027";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = OrderDetailFilterFunction.getDwdOrderDetail(env, groupId);

        //todo 将每行数据转换为javabean
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceDS = orderDetailJsonObjDS.map(json -> {
            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(json.getString("order_id"));


            return new TradeProvinceOrderWindow(
                    "",
                    "",
                    json.getString("province_id"),
                    "",
                    orderIdSet,
                    0L,
                    json.getDouble("split_total_amount"),
                    DateFormatUtil.toTs(json.getString("order_create_time"), true));

        });
        //todo 提取时间戳生成watermark

        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWithWmDS = tradeProvinceDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                    @Override
                    public long extractTimestamp(TradeProvinceOrderWindow value, long l) {
                        return value.getTs();
                    }
                }));

        //todo 分组开窗聚合
        KeyedStream<TradeProvinceOrderWindow, String> keyedStream = tradeProvinceWithWmDS.keyBy(TradeProvinceOrderWindow::getProvinceId);
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                value1.setOrderCount(value1.getOrderCount() + value1.getOrderCount());
                //合并订单ID的集合
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                return value1;
            }
        }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> iterable, Collector<TradeProvinceOrderWindow> collector) throws Exception {
                //获取数据
                TradeProvinceOrderWindow value = iterable.iterator().next();
                //补充信息
                value.setTs(System.currentTimeMillis());
                value.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                value.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                value.setOrderCount((long) value.getOrderIdSet().size());
                //输出数据
                collector.collect(value);
            }
        });
        //todo 关联为表获取省份名称（当分组条件单一的时候可以先聚合，再关联，提高效率）
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWithProvinceNameDS = AsyncDataStream.unorderedWait(reduceDS
        , new DimAsyncFunction<TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindow input, JSONObject dimInfo) {
                        input.setProvinceName(dimInfo.getString("NAME"));
                    }
                }
        ,60
        , TimeUnit.SECONDS);

        //todo 将数据写出到ck
        tradeProvinceWithProvinceNameDS.print(">>>>>>>>>>");
        //todo 启动
        env.execute("DwsTradeProvinceOrderWindow");




    }
}
