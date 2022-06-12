package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.bean.UserLoginBean;
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
import org.apache.flink.util.Collector;

import java.io.FilterWriter;
import java.time.Duration;

public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);


        //todo 读取kafka，页面日志主题，创建流
        //页面主题
        String page_topic = "dwd_traffic_page_log";

        String groupId = "dws_user_user_login_window";
        //创建流
        DataStreamSource<String> pageStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(page_topic, groupId));


        //todo 将数据转换成json对象
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = pageStringDS.map(JSON::parseObject);
        //todo 过滤数据，uid不为null and last_page_id 为 null
        //使用FlatMap代替
        SingleOutputStreamOperator<JSONObject> filterDS = pageStringDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                if (common.getString("uid") != null && page.getString("last_page_id") == null) {
                    out.collect(jsonObject);
                }
            }
        });

        //todo 提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> filterWithWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));
        //todo 按照uid分组
        KeyedStream<JSONObject, String> keyedStream = filterWithWmDS.keyBy(json -> json.getJSONObject("common").getString("uid"));
        //todo 使用状态编程实现 回流及独立用户的提取

        SingleOutputStreamOperator<UserLoginBean> uvDS = keyedStream.process(new KeyedProcessFunction<String,JSONObject, UserLoginBean>() {


            private ValueState<String> lastVisitDt;
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject,Context ctx, Collector<UserLoginBean> collector) throws Exception {
                //取出状态中的数据，即上次保存的日期
                String lastDt = lastVisitDt.value();
                //获取当前数据中的时间并转换为日期
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);

                //定义独立用户&回流用户数
                long uuCt = 0L;
                long backCt = 0L;
                //状态保存的日期为null，则表示为新用户
                if(lastDt == null){
                    uuCt = 1L;

                }else {
                    //状态保存的日期不为null，且与当前数据日期不同，则为今天第一条数据
                    if(!lastDt.equals(curDt)){
                        uuCt = 1L;

                        //如果保存的日期与当前数据日期差值大于等于8,则为回流用户
                        Long lastTs = DateFormatUtil.toTs(lastDt);
                        long days = (ts - lastTs) / (1000L * 60 * 60 * 24);
                        if (days >= 8L) {
                            backCt = 1;
                        }
                    }
                }
                lastVisitDt.update(curDt);
                //判断,如果当日独立用户数为 1,则输出
                if (uuCt == 1L) {
                    collector.collect(new UserLoginBean("",
                            "",
                            backCt,
                            uuCt,
                            System.currentTimeMillis()));
                }
            }
        });

        //todo 开窗、聚合

        DataStream<UserLoginBean> resultDS = uvDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        //取出数据
                        UserLoginBean userLoginBean = iterable.iterator().next();

                        //补充窗口信息
                        userLoginBean.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        userLoginBean.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                        //输出数据
                        collector.collect(userLoginBean);
                    }
                });

        //todo 将数据写入ck
        resultDS.print(">>>>>>>>>>>>.");

        //todo 启动任务

        env.execute("DwsUserUserLoginWindow");


    }
}
