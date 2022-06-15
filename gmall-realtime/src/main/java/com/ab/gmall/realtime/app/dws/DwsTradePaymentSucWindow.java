package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.app.func.OrderDetailFilterFunction;
import com.ab.gmall.realtime.bean.TradePaymentWindowBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);

        //todo 读取dwd 成功支付数据主题
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_province_order_window";
        DataStreamSource<String> paymentStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));
        //todo 过滤数据转换json对象

        SingleOutputStreamOperator<JSONObject> jsonObjDS =  paymentStrDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("value : " + s + "-----");
                }
            }
        });

        //todo 按照唯一键分组，支付id

        KeyedStream<JSONObject, String> keyedByDetailIdStream =  jsonObjDS.keyBy(json  -> json.getString("order_detail_id"));

        //todo 使用状态编程，保留第一条数据
        SingleOutputStreamOperator<JSONObject> filterDS =  keyedByDetailIdStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String state = valueState.value();
                if(state == null){
                    valueState.update("1");
                    return true;
                }else {

                    return false;
                }

            }
        });

        //todo 按照uid分组
        KeyedStream<JSONObject, String> keyedByUserStream = filterDS.keyBy(jsonObject -> jsonObject.getString("user_id"));

        //todo 提取当日以及新增支付人数
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedByUserStream.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            private ValueState<String> lastPaymentDtState;
            @Override
            public void open(Configuration parameters) throws Exception {
                lastPaymentDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-payment", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                 //取出状态数据
                String lastPaymentDt = lastPaymentDtState.value();
                //取出当前数据日期
                String callbackTime = jsonObject.getString("callback_time");
                String curDt = callbackTime.split(" ")[0];

                //定义两个数字
                long paymentSucUniqueUserCount = 0L;
                long paymentSucNewUserCount = 0L;

                if (lastPaymentDt == null) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;
                    lastPaymentDtState.update(curDt);
                } else if (!lastPaymentDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastPaymentDtState.update(curDt);
                }
                if (paymentSucUniqueUserCount == 1L) {
                    collector.collect(new TradePaymentWindowBean("",
                            "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            DateFormatUtil.toTs(callbackTime, true)));
                }
            }
        });
        //todo 提取时间时间、开窗聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> reduceDS = tradePaymentDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradePaymentWindowBean>() {
                    @Override
                    public long extractTimestamp(TradePaymentWindowBean tradePaymentWindowBean, long l) {
                        return tradePaymentWindowBean.getTs();
                    }
                }))
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        value2.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value1.getPaymentSucUniqueUserCount());
                        return value2;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> collector) throws Exception {
                        //取出数据
                        TradePaymentWindowBean windowBean = values.iterator().next();
                        //补充信息
                        windowBean.setTs(System.currentTimeMillis());
                        windowBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        windowBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        //输出数据
                        collector.collect(windowBean);
                    }
                });

        //todo 将数据写到ck
        reduceDS.print(">>>>>>>>>>>");

        //todo 启动任务
        env.execute("DwsTradePaymentSucWindow");
    }
}
