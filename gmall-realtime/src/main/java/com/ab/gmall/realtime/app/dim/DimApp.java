package com.ab.gmall.realtime.app.dim;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp {
    public static void main(String[] args) {
        //todo 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);//生成环境设置成kafka主题的分区数
        //配置ck
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setCheckpointStorage("hdfs:hadoop100:9820//flink/checkpoint/dimapp");

    }
}
