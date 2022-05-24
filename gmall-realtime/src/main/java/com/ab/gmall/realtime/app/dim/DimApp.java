package com.ab.gmall.realtime.app.dim;

import com.ab.gmall.realtime.app.func.DimSinkFunction;
import com.ab.gmall.realtime.app.func.TableProcessFunction;
import com.ab.gmall.realtime.bean.TableProcess;
import com.ab.gmall.realtime.util.MyKafkaUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class DimApp {
    public static void main(String[] args) throws Exception {
        //todo 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);//生成环境设置成kafka主题的分区数
//        // todo 2. 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop100:9820/gmall/ck");
//        System.setProperty("HADOOP_USER_NAME", "root");
        //todo 3.读取业务
        String topic = "maxwell";
        String groupId = "dim_app_2205";
        DataStreamSource<String> gmallDS  = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));
        //todo 4.主流ETL，过滤非JSON格式的数据写入侧输出，过滤内容不是数据的
        OutputTag<String> dirtyDataTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = gmallDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    //过滤内容不是数据的
                    if(jsonObject.getString("type").equals("bootstrap-start")
                            || jsonObject.getString("type").equals("bootstrap-complete")) {
                        context.output(dirtyDataTag, s);
                    }else {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    context.output(dirtyDataTag, s);
                }
            }
        });

        // 打印测试
//        jsonObjDS.print("filterDS >>> ");
//        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyDataTag);
//        sideOutput.print("DIRTY >>> ");
        //todo 5.使用flinkcdc读取Mysql中的配置信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .deserializer( new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");
        mysqlSource.print();
        //todo 6.将配置信息处理成广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSource.broadcast(mapStateDescriptor);

        //todo 7.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectStream = jsonObjDS.connect(broadcastStream);

        //todo 8.根据广播流数据处理主流数据
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectStream.process(new TableProcessFunction(mapStateDescriptor));

        //TODO 8.将数据写出到Phoenix中
        hbaseDS.print(">>>>>>>>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        env.execute();

    }
}
