package com.ab.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class MyKafkaUtil {
    private static Properties properties = new Properties();
    private static final String BOOTSTRAP_SERVERS = "hadoop100:9092,hadoop101:9092,hadoop102:9092";
    static String DEFAULT_TOPIC = "default_topic";
    static {
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
    }
    //Kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic , String group_id){
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        return new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord == null || consumerRecord.value() == null) {
                    return "";
                } else {
                    return new String(consumerRecord.value());
                }
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        },
                properties);
    }
    //Kafka生产者
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }
    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset')";
    }
    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

    /**
     * 创建消费db的kafkaddl
     * @param groupId
     * @return
     */
    public static String getTopicDbDDL(String groupId) {
        return "CREATE TABLE maxwell ( " +
                "  `database` String, " +
                "  `table` String, " +
                "  `type` String, " +
                "  `data` Map<String,String>, " +
                "  `old` Map<String,String>, " +
                "  `pt` AS PROCTIME() " +
                ")" + MyKafkaUtil.getKafkaDDL("maxwell", groupId);
    }
}
