package com.yeyangshu.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 创建一个消费者，指定分组
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2020/12/10 23:54
 */
public class ConsumerOffset {
    public static void main(String[] args) {
        // 1 配置属性信息
        Properties properties = new Properties();
        // bootstrap.servers，服务器参数
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        // key、value反序列化
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // group.id，分组
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        // auto.offset.reset，默认提交方式
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 2 创建Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3 消费者订阅topics
        consumer.subscribe(Arrays.asList("topic01"));
        consumer.assign(Arrays.asList(new TopicPartition("topic01", 1)));

        // 4 拉取消息并消费
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                if (!consumerRecords.isEmpty()) {
                    Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();
                    while (recordIterator.hasNext()) {
                        ConsumerRecord<String, String> record = recordIterator.next();
                        String topic = record.topic();
                        int partition = record.partition();
                        long offset = record.offset();
                        String key = record.key();
                        String value = record.value();
                        long timestamp = record.timestamp();
                        System.out.println(topic + "\t" + partition + "\t" + key + "\t" + value + timestamp);
                    }
                }
            }
        } finally {
            // 关闭消费者实例
            consumer.close();
        }
    }
}
