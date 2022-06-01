package com.ab.gmall.realtime.app.dwd.log;

import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //todo 1.初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //todo 启用状态后端
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop100:9820/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "root");
        //todo 3.从Kafka读取主流数据
        String topic = "topic_log";
        String groupId = "consumer";
        DataStreamSource<String> source = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));
        //todo 4.etl,转换结构
        // 4.1 定义错误侧输出流
        OutputTag<String> dirtyStreamTag = new OutputTag<String>("dirtyStream"){};
        SingleOutputStreamOperator<JSONObject> cleanedStream = source.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyStreamTag, s);
                }
            }
        });
        //todo 4.2 将脏数据写入到Kafka指定主题
        DataStream<String> sideOutput = cleanedStream.getSideOutput(dirtyStreamTag);
        String dirtyTopic = "dirty_data";
        sideOutput.addSink(MyKafkaUtil.getKafkaProducer(dirtyTopic));

        //todo 5.新老客状态标记修复
        //5.1 按照mid对数据进行分组
        KeyedStream<JSONObject, String> keyedStream = cleanedStream.keyBy(row -> row.getJSONObject("common").getString("mid"));
        //5.2 新老访客状态标记修复
        SingleOutputStreamOperator<JSONObject> fixedStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstViewDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                firstViewDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>(("lastLoginDt"), String.class));


            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                String firstViewDt = firstViewDtState.value();
                Long ts = jsonObject.getLong("ts");
                String dt = DateFormatUtil.toDate(ts);
                if ("1".equals(isNew)) {
                    if (firstViewDt == null) {
                        firstViewDtState.update(dt);
                    } else {
                        if (!firstViewDt.equals(dt)) {
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    if (firstViewDt == null) {
                        //将首次访问日期置为昨天
                        String yesterday = DateFormatUtil.toDate(ts - 1000 * 60 * 60 * 24);
                        firstViewDtState.update(yesterday);
                    }
                }
                collector.collect(jsonObject);
            }

        });
        //todo 6.分流
        // 6.1 定义启动、曝光、动作、错误侧输出流
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag"){};
        OutputTag<String> errorTag = new OutputTag<String>("errorTag"){};
        //todo 6.2分流
        SingleOutputStreamOperator<String> separatedStream  = fixedStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //todo 6.2.1 收集错误数据
                JSONObject err = jsonObj.getJSONObject("err");
                if (err != null) context.output(errorTag, jsonObj.toJSONString());
                jsonObj.remove("err");
                JSONObject start = jsonObj.getJSONObject("start");
                if (start != null) {
                    context.output(startTag, jsonObj.toJSONString());
                } else {
                    JSONObject page = jsonObj.getJSONObject("page");
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");
                    //todo 6.2.3 收集曝光数据
                    JSONArray displays = jsonObj.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            JSONObject displayObj = new JSONObject();
                            displayObj.put("display", display);
                            displayObj.put("common", common);
                            displayObj.put("page", page);
                            displayObj.put("ts", ts);
                            context.output(displayTag, displayObj.toJSONString());
                        }
                    }
                    //todo 6.2.4 收集动作数据
                    JSONArray actions = jsonObj.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            JSONObject actionObj = new JSONObject();
                            actionObj.put("action", action);
                            actionObj.put("common", common);
                            actionObj.put("page", page);
                            actionObj.put("ts", ts);
                            context.output(actionTag, actionObj.toJSONString());
                        }
                    }
                    //todo 6.2.5 收集页面数据
                    jsonObj.remove("displays");
                    jsonObj.remove("actions");
                    collector.collect(jsonObj.toJSONString());
                }
            }
        });
        //todo 7.将数据输出到kafka的不同主题
        //测试输出
        separatedStream.print("page>>>>>>>>>>>");
//        separatedStream.getSideOutput(startTag).print("start>>>>>>>>>>");
//        separatedStream.getSideOutput(displayTag).print("display>>>>>>>>>>");
//        separatedStream.getSideOutput(actionTag).print("action>>>>>>>>>>");
//        separatedStream.getSideOutput(errorTag).print("errot>>>>>>>>>>");

//        separatedStream.print("page>>>>>>>>>>>");
        DataStream<String> startDS = separatedStream.getSideOutput(startTag);
        DataStream<String> displayDS = separatedStream.getSideOutput(displayTag);
        DataStream<String> actionDS = separatedStream.getSideOutput(actionTag);
        DataStream<String> errorDS = separatedStream.getSideOutput(errorTag);
        // 7.2 定义不同日志输出到 Kafka 的主题名称
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        separatedStream.addSink(MyKafkaUtil.getKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getKafkaProducer(error_topic));

        env.execute();


    }
}
