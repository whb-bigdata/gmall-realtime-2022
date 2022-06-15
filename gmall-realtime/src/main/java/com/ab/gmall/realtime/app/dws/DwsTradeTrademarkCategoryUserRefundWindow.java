package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.app.func.DimAsyncFunction;
import com.ab.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        //todo 读取DWD层退单主题数据创建流
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window_211027";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        //todo 转换为JSON对象，过滤去重数据（使用状态编程取第一条）
        SingleOutputStreamOperator<JSONObject> filterDS =  kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                if(!"".equals(s)){
                    collector.collect(JSONObject.parseObject(s));
                }
            }
        })
                .keyBy(json  -> json.getString("id"))
                .filter(new RichFilterFunction<JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value-state", String.class);
                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        stateDescriptor.enableTimeToLive(ttlConfig);
                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        //取出状态
                        String state = valueState.value();
                        if(state == null){
                            valueState.update("1");
                            return true;
                        } else {
                            return false;
                        }
                    }
                });



        //todo 将每行数据转换为javaBean对象
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTrademarkDS = filterDS.map(json ->
                TradeTrademarkCategoryUserRefundBean
                        .builder()
                        .skuId(json.getString("sku_id"))
                        .userId(json.getString("user_id"))
                        .refundCount(1L)
                        .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                        .build());
        //todo 关联维表
        //SKU
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithSkuDS = AsyncDataStream.unorderedWait(tradeTrademarkDS
                , new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        input.setTrademarkId(dimInfo.getString("TM_ID"));
                    }
                }
                ,60
                , TimeUnit.SECONDS);
        //Trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithTradeMarkDS =  AsyncDataStream.unorderedWait(beanWithSkuDS
        , new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                }
                ,60
        ,TimeUnit.SECONDS);

        //Category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithCategory3DS = AsyncDataStream.unorderedWait(
                beanWithTradeMarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //Category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithCategory2DS = AsyncDataStream.unorderedWait(
                beanWithCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                }, 60, TimeUnit.SECONDS);
        //Category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithCategory1DS = AsyncDataStream.unorderedWait(
                beanWithCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);
        //todo 提取水位线生成watermark 、 分组开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = beanWithCategory1DS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, long l) {
                        return tradeTrademarkCategoryUserRefundBean.getTs();
                    }
                })).keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                return  value.getSkuId() + "-" +
                        value.getTrademarkId() + "-" +
                        value.getTrademarkName() + "-" +
                        value.getCategory1Id() + "-" +
                        value.getCategory1Name() + "-" +
                        value.getCategory2Id() + "-" +
                        value.getCategory2Name() + "-" +
                        value.getCategory3Id() + "-" +
                        value.getCategory3Name();
                }
            }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.setRefundCount(value1.getRefundCount() + value2.getRefundCount());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                        //获取数据
                        TradeTrademarkCategoryUserRefundBean refundBean = input.iterator().next();

                        //补充信息
                        refundBean.setTs(System.currentTimeMillis());
                        refundBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        refundBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));

                        //输出数据
                        collector.collect(refundBean);
                    }
                });


        //todo 将数据写出道ck
        reduceDS.print(">>>>>>>>>>>>>>>>>>>");
        //todo 启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");

    }




}
