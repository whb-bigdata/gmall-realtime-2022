package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.app.func.DimAsyncFunction;
import com.ab.gmall.realtime.app.func.OrderDetailFilterFunction;
import com.ab.gmall.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

public class DwsTradeTrademarkCategoryUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {
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
        //打印测试
        skuUserOrderDS.print(">>>>>");
        //todo 关联维表，异步IO方法
        //关联SKU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSkuDS = AsyncDataStream.unorderedWait(skuUserOrderDS
                , new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setSpuId(dimInfo.getString("SPU_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }
                }
                , 60
                , TimeUnit.SECONDS);

        //关联SPU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSpuDS = AsyncDataStream.unorderedWait(withSkuDS
                , new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setSpuName(dimInfo.getString("SPU_NAME"));
                    }
                }
                , 60
                , TimeUnit.SECONDS);
        //关联TM
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withTmDS = AsyncDataStream.unorderedWait(withSpuDS
                , new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setTrademarkId(dimInfo.getString("TM_NAME"));
                    }
                }
                , 60
                , TimeUnit.SECONDS);

        //关联Category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory3DS = AsyncDataStream.unorderedWait(withTmDS
                , new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                        input.setCategory3Name(dimInfo.getString("NAME"));
                    }
                }
                , 60
                , TimeUnit.SECONDS);

        //关联Category2

        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory2DS = AsyncDataStream.unorderedWait(withCategory3DS
                , new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                }
                , 60
                , TimeUnit.SECONDS);

        //关联Category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1DS = AsyncDataStream.unorderedWait(withCategory2DS
                , new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                }
                , 60
                , TimeUnit.SECONDS);

        //打印测试
        withCategory1DS.print("withCategory1DS>>>>>>>>");

        //todo 提取时间戳生成watermark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> tradeTrademarkCategoryUserSpuOrderWithWmDS = withCategory1DS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeTrademarkCategoryUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>() {
                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserSpuOrderBean tradeTrademarkCategoryUserSpuOrderBean, long l) {
                        return tradeTrademarkCategoryUserSpuOrderBean.getTs();
                    }
                }));
        //todo 分组开窗聚合
        //直接用一个string来当key，不用tuple,原因：字段太多
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean>  reduceDS = tradeTrademarkCategoryUserSpuOrderWithWmDS.keyBy(new KeySelector<TradeTrademarkCategoryUserSpuOrderBean, String>() {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {
                        return value.getUserId() + "-" +
                                value.getCategory1Id() + "-" +
                                value.getCategory1Name() + "-" +
                                value.getCategory2Id() + "-" +
                                value.getCategory2Name() + "-" +
                                value.getCategory3Id() + "-" +
                                value.getCategory3Name() + "-" +
                                value.getSpuId() + "-" +
                                value.getSpuName() + "-" +
                                value.getTrademarkId() + "-" +
                                value.getTrademarkName();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new RichReduceFunction<TradeTrademarkCategoryUserSpuOrderBean>() {
                    @Override
                    public TradeTrademarkCategoryUserSpuOrderBean reduce(TradeTrademarkCategoryUserSpuOrderBean value1, TradeTrademarkCategoryUserSpuOrderBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserSpuOrderBean> input, Collector<TradeTrademarkCategoryUserSpuOrderBean> collector) throws Exception {
                        //获取数据
                        TradeTrademarkCategoryUserSpuOrderBean orderBean = input.iterator().next();

                        //补充信息
                        orderBean.setTs(System.currentTimeMillis());
                        orderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        orderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());

                        //输出数据
                        collector.collect(orderBean);
                    }
                });
        //todo 将数据写出到ck
        reduceDS.print(">>>>>>>>>>>>>");
        //todo 启动任务
        env.execute("DwsTradeTrademarkCategoryUserSpuOrderWindow");

    }
}
