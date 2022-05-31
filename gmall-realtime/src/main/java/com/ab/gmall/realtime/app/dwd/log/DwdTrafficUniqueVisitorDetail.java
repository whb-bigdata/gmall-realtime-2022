package com.ab.gmall.realtime.app.dwd.log;

import com.ab.gmall.realtime.util.DateFormatUtil;
import com.ab.gmall.realtime.util.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        //todo 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生成环境设置为Kafka主题的分区数

        //todo 读取Kafka数据，创建流
        String sourcePageTopic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_unique_visitor_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourcePageTopic, groupId));

        //todo 转化为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //todo 过滤掉上一跳页面id不等于null的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        });

        //todo 按照Mid进行分组
        KeyedStream<JSONObject, String> keyedByMidStream = filterDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //todo 使用状态编程进行每日登陆数据去重
        SingleOutputStreamOperator<JSONObject> uvDetailDS = keyedByMidStream.filter(new RichFilterFunction<JSONObject>() {
            //定一个浏览状态
            private ValueState<String> visitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("visite-dt", String.class);
                //设置状态的ttl
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                //进行初始化
                visitDtState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出状态以及当前数据的日期
                String dt = visitDtState.value();
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));
                //如果状态数据为null或者状态日期与当前数据日期不同，则保留数据，同时更新数据，状态数据不为null并且dt为今天，那么说明这个mid今天来过
                if (dt == null || !dt.equals(curDt)) {
                    visitDtState.update(curDt);
                    return true;
                } else {
                    return false;
                }
            }
        });
        //todo 将数据写到kafka中
        uvDetailDS.print(">>>>>>>>>>>>>>");
        String targetTopic = "dwd_traffic_unique_visitor_detail";
//        uvDetailDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(targetTopic));

        //todo 启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");

    }
}
