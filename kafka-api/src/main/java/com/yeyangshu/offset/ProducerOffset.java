package com.yeyangshu.offset;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

/**
 * 创建一个生产者
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2020/12/10 23:39
 */
public class ProducerOffset {
    public static void main(String[] args) {
        // 1 配置属性信息
        Properties properties = new Properties();
        // bootstrap.servers，服务器参数
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        // key、value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2 创建Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3 生产者生产消息
        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "key" + i, "value" + i);
            // 发送消息给服务器
            producer.send(record);
        }

        // 4 关闭生产者
        producer.close();
    }
}
