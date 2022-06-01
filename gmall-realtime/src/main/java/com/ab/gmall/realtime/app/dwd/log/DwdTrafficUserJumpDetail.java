package com.ab.gmall.realtime.app.dwd.log;

import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //todo 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //todo 读取kafka主题
        String sourcePageTopic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourcePageTopic, groupId));

        //todo 将数据转换为JSON对象

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //todo 提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
        //todo 按照mid进行分组
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjWithWmDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //todo 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("second").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));
        //todo 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedByMidStream, pattern);

        //todo 提取匹配上的事件以及超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("time-out") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("first").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("first").get(0);
            }
        });

        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //todo 合并两个流
        selectDS.print("Select>>>>>>>>>");
        timeOutDS.print("TimeOut>>>>>>>>>");
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        //todo 将数据写出到Kafka
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(targetTopic));

        //todo 启动任务
        env.execute("DwdTrafficUserJumpDetail");

    }
}
