package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
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

public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {

        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);


        //todo 读取kafka，页面日志主题，创建流
        //页面主题
        String page_topic = "dwd_traffic_page_log";

        String groupId = "dws_traffic_page_view_window";
        //创建流
        DataStreamSource<String> pageStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(page_topic, groupId));


        //todo 将数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = pageStringDS.map(JSON::parseObject);

        //todo 过滤数据，只需访问主页和商品详情页的数据

        SingleOutputStreamOperator<JSONObject> homeAndDetailPageDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                return "good_detail".equals(pageId) || "home".equals(pageId);
            }
        });

        //todo 提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> homeAndDetailPageWithWmDS = homeAndDetailPageDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));

        //todo 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = homeAndDetailPageWithWmDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));


        //todo 使用状态编程，计算主页和商品详情页的独立访客

        DataStream<TrafficHomeDetailPageViewBean> trafficHomeDetailDS =  keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastVisitDt;
            private ValueState<String> detailLastVisitDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                //初始化home页面访问日期状态
                ValueStateDescriptor<String> homeDtDescriptor = new ValueStateDescriptor<>("home-dt", String.class);
                homeDtDescriptor.enableTimeToLive(stateTtlConfig);
                homeLastVisitDt = getRuntimeContext().getState(homeDtDescriptor);
                //初始化商品详情页访问日期状态
                ValueStateDescriptor<String> detailDtDescriptor = new ValueStateDescriptor<>("detail-dt", String.class);
                detailDtDescriptor.enableTimeToLive(stateTtlConfig);
                detailLastVisitDt = getRuntimeContext().getState(detailDtDescriptor);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //取出当前页面信息及时间戳
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                //定义主页以及商品详情页的访问次数
                long homeUvCt = 0L;
                long detailUvCt = 0L;

                //判断是否为主页数据
                if("home".equals(pageId)){
                    //判断状态以及当前日期是否相同，如果不相同或者没有状态数据，则需要homeUvCt=1
                    String homeLastDt = homeLastVisitDt.value();
                    if(homeLastDt == null || !homeLastDt.equals(curDt)){
                        homeUvCt = 1L;
                        homeLastVisitDt.update(curDt);
                    }
                }else {//商品详情页
                    //判断状态以及与当前日期是否相同
                    String detailLastDt = detailLastVisitDt.value();
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        detailUvCt = 1L;
                        detailLastVisitDt.update(curDt);
                    }
                }
                if(homeUvCt != 0L || detailUvCt != 0L){
                    collector.collect(new TrafficHomeDetailPageViewBean("","",
                            homeUvCt,
                            detailUvCt,
                            System.currentTimeMillis()));
                }


            }
        });

        //todo 开窗，聚合
        DataStream<TrafficHomeDetailPageViewBean> reduceDS = trafficHomeDetailDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //获取数据
                TrafficHomeDetailPageViewBean pageViewBean = values.iterator().next();

                //设置窗口信息
                pageViewBean.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                pageViewBean.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                //输出数据
                collector.collect(pageViewBean);
            }
        });

        //todo 将数据输出到ck
        reduceDS.print(">>>>>>>>>>");

        //todo 启动任务
        env.execute("DwsTrafficPageViewWindow");
    }
}
