package com.ab.gmall.realtime.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MyEnv {
    public static StreamExecutionEnvironment getEnv(int parallelism){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        //env.setStateBackend(new HashMapStateBackend());
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");
        return env;
    }
}
