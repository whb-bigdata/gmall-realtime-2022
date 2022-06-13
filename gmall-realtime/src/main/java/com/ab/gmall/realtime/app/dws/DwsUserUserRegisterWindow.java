package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.bean.UserRegisterBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);


        //todo 读取kafka，页面日志主题，创建流
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window_211027";
        DataStreamSource<String> registerDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));


        //todo 将数据转换成javabean对象
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = registerDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return new UserRegisterBean("", "", 1L, jsonObject.getLong("ts") * 1000L);
        });
        //todo 提取时间戳生成watermark
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWmDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                        return userRegisterBean.getTs();
                    }
                }));
        //todo 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = userRegisterWithWmDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                        //提取数据
                        UserRegisterBean registerBean = values.iterator().next();

                        //补充窗口信息
                        registerBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        registerBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        //输出数据
                        out.collect(registerBean);
                    }
                });
        //todo 将数据写入到clickhouse
        resultDS.print(">>>>>>>>>>");

        //todo 启动任务
        env.execute("DwsUserUserRegisterWindow");

    }



}
