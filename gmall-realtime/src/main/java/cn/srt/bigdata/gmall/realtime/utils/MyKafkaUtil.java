package cn.srt.bigdata.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Kafka工具类
 */
public class MyKafkaUtil {

    //1.定义kafka的bootstrap-server
    private static final String bootStrapServer = "bigdata-test01:9092,bigdata-test02:9092,bigdata-test03:9092";

    /**
     * flink消费kafka主题
     *
      * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaSouce(String topic,String groupId) {
        //1.定义属性
        Properties properties = new Properties();
        //2.设置属性
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        return flinkKafkaConsumer;
    }

    /**
     * flink生产者发往某个topic主题
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer getKafkaSink(String topic) {

        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer(bootStrapServer, topic, new SimpleStringSchema());

        return  flinkKafkaProducer;
    }
}
