package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.bean.TradeOrderBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.ab.gmall.realtime.util.TimestampLtz3CompareUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);
        //todo 读取Dwd层 订单明细数据
        String groupId = "dws_trade_order_window";
        String topic = "dwd_trade_order_detail";
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //todo 过滤（""不需要，保留"insert"），转换数据为JSON
        SingleOutputStreamOperator<JSONObject> jsonObjDS = orderDetailStrDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (!"".equals(value)) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    if ("insert".equals(jsonObject.getString("type"))) {
                        out.collect(jsonObject);
                    }
                }
            }
        });
        //todo 按照order_detail_id进行分组
        KeyedStream<JSONObject, String> keyedByOrderDetailIdStream = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));

        //todo 去重并返回返回结果
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjDS = keyedByOrderDetailIdStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> orderDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                orderDetailState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("order-detail", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                //获取状态数据并判断是否有数据
                JSONObject orderDetail = orderDetailState.value();

                if (orderDetail == null) {
                    //把当前数据设置进状态并且注册定时器
                    orderDetailState.update(value);
                    //定一个处理时间的定时器，处理时间+2秒
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
                } else {

                    //2022-04-01 11:10:55.042Z
                    String stateTs = orderDetail.getString("ts");
                    //2022-04-01 11:10:55.9Z
                    String curTs = value.getString("ts");

                    int compare = TimestampLtz3CompareUtil.compare(stateTs, curTs);
                    if (compare != 1) {  //表示后到的数据时间大
                        //更新状态
                        orderDetailState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //提取状态数据并输出
                JSONObject orderDetail = orderDetailState.value();
                out.collect(orderDetail);
            }
        });
        //todo 提取时间戳，生成watermark

        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = orderDetailJsonObjDS
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return DateFormatUtil.toTs(jsonObject.getString("order_create_time"),true);
                            }
                        })
        );

        //todo 按照user_id 分组
        KeyedStream<JSONObject, String> keyedByUidStream = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));
        //todo 提取下单独立用户并转化为javabean
        DataStream<TradeOrderBean> tradeOrderDS = keyedByUidStream.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
            private ValueState<String> lastOrderDt;
            @Override
            public void open(Configuration parameters) throws Exception {
                //这个状态就是永久的了，因为要统计总的，只要有历史，总的就不用+1
             lastOrderDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order",String.class));
            }

            @Override
            public void flatMap(JSONObject value, Collector<TradeOrderBean> collector) throws Exception {
                //取出状态时间
                String lastOrder = lastOrderDt.value();
                //取出当前数据下单日期,，只取年月日
                String curDt = value.getString("order_create_time").split(" ")[0];
                //定义独立下单数以及新增下单数
                long orderUniqueUserCount = 0L;
                long orderNewUserCount = 0L;
                //开始判断
                if(lastOrder == null ){
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;
                    lastOrderDt.update(curDt);
                }else  if (!lastOrder.equals(curDt)){
                    orderUniqueUserCount = 1L;
                    lastOrderDt.update(curDt);
                }
                //输出结果
                Double activityReduceAmount = value.getDouble("activity_reduce_amount");
                if (activityReduceAmount == null) {
                    activityReduceAmount = 0.0D;
                }

                Double couponReduceAmount = value.getDouble("coupon_reduce_amount");
                if (couponReduceAmount == null) {
                    couponReduceAmount = 0.0D;
                }
                collector.collect(new
                        TradeOrderBean("", "",
                        orderUniqueUserCount,
                        orderNewUserCount,
                        activityReduceAmount,
                        couponReduceAmount,
                        value.getDouble("original_total_amount"),
                        0L));
            }
        });

        //todo 开窗，聚合
        DataStream<TradeOrderBean> resultDS = tradeOrderDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + value2.getOrderActivityReduceAmount());
                        value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + value2.getOrderCouponReduceAmount());
                        value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + value2.getOrderOriginalTotalAmount());
                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        //取出数据
                        TradeOrderBean orderBean = values.iterator().next();

                        //补充时间
                        orderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        orderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        orderBean.setTs(System.currentTimeMillis());

                        //输出数据
                        out.collect(orderBean);
                    }
                });

        //todo 输出到ck
        resultDS.print(">>>>>>>>>>>>>");
        //todo 启动任务
        env.execute("DwsTradeOrderWindow");
    }





}
