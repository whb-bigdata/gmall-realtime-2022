package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.bean.CartAddUuBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);


        //todo 读取kafka，Kafka DWD层加购数据主题，创建流
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> cartAddStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));
        //todo 将每行转为json格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = cartAddStringDS.map(JSON::parseObject);

        //todo 提取时间戳生成watermark

        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        String dateTime = jsonObject.getString("operate_time");
                        if (dateTime == null) {
                            dateTime = jsonObject.getString("create_time");
                        }

                        return DateFormatUtil.toTs(dateTime, true);
                    }
                }));

        //todo 按照user_id 进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(line -> line.getString("user_id"));
        //todo 过滤戳独立用户，同时转换数据结构
        SingleOutputStreamOperator<CartAddUuBean> cartAddDs = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            private ValueState<String> lastCartAddDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("cart-add", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastCartAddDt = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> collector) throws Exception {
                //提取状态数据
                String lastDt = lastCartAddDt.value();
                //yyyy-MM-dd HH:mm:ss
                String dateTime = value.getString("operate_time");
                if (dateTime == null) {
                    dateTime = value.getString("create_time");
                }
                String curDt = dateTime.split(" ")[0];

                //如果状态数据为null或者与当前日期不是同一天,则保留数据,更新状态
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartAddDt.update(curDt);
                    collector.collect(new CartAddUuBean("", "", 1L, 0L));
                }

            }
        });
        //todo 开窗聚合
        DataStream<CartAddUuBean> resultDS = cartAddDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        //取出数据
                        CartAddUuBean cartAddUuBean = iterable.iterator().next();
                        //补充窗口信息
                        cartAddUuBean.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        cartAddUuBean.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        //补充版本信息
                        cartAddUuBean.setTs(System.currentTimeMillis());
                        //输出数据
                        collector.collect(cartAddUuBean);

                    }
                });

        //todo 写出到ck
        resultDS.print(">>>>>>>>>>>>");

        //todo 执行
        env.execute("DwsTradeCartAddUuWindow");

    }
}
