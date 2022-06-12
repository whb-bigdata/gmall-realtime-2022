package com.ab.gmall.realtime.app.dws;

import com.ab.gmall.realtime.bean.TrafficPageViewBean;
import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyEnv;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = new MyEnv().getEnv(4);


        //todo 读取3个主题数据形成3个流
        //页面主题
        String page_topic = "dwd_traffic_page_log";
        //uv的主题
        String uv_topic = "dwd_traffic_unique_visitor_detail";
        //uj的主题
        String uj_topic = "dwd_traffic_user_jump_detail";
        String groupId = "dws_traffic_vc_ch_ar_isnew_page_view_window";
        //创建流
        DataStreamSource<String> pageStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(page_topic, groupId));
        DataStreamSource<String> uvStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uv_topic, groupId));
        DataStreamSource<String> ujStringDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uj_topic, groupId));

        //todo 将三个流统一数据格式
        //处理uv数据
        DataStream<TrafficPageViewBean> trafficPageViewWithUvDS = uvStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });
        //处理uj数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUjDS = ujStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    jsonObject.getLong("ts"));
        });
        //处理page数据
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPvDS = pageStringDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            JSONObject page = jsonObject.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");
            //有上一跳，初始会话数就为0，没有上一跳，就是一个新的会话，此处就应该为1
            long sv = 0L;
            if (lastPageId == null) {
                sv = 1;
            }

            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    sv,
                    1L,
                    page.getLong("during_time"),//直接可以获取during_time
                    0L,
                    jsonObject.getLong("ts"));
        });

        //todo 合并三个流，提取事件时间生成watermark
        DataStream<TrafficPageViewBean> unionDS = trafficPageViewWithUvDS.union(trafficPageViewWithUjDS, trafficPageViewWithPvDS)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                        return trafficPageViewBean.getTs();
                    }
                }));
        //todo 分组、开窗、聚合
        DataStream<TrafficPageViewBean> reduceDS = unionDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4 getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                        return new Tuple4(trafficPageViewBean.getAr()
                                , trafficPageViewBean.getCh()
                                , trafficPageViewBean.getIsNew()
                                , trafficPageViewBean.getVc());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        return value1;
                    }
                }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                        //获取数据
                        TrafficPageViewBean trafficPageViewBean = iterable.iterator().next();
                        //获取窗口信息
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        //补充窗口信息
                        trafficPageViewBean.setStt(DateFormatUtil.toYmdHms(start));
                        trafficPageViewBean.setEdt(DateFormatUtil.toYmdHms(end));
                        //输出数据
                        collector.collect(trafficPageViewBean);
                    }
                });

        //todo 将数据写出到clickhouse
        reduceDS.print(">>>>>>>>>>>>>>>");


        //todo 启动任务
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");

    }
}
