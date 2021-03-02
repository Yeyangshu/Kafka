package com.yeyangshu.quickstart;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * 创建一个消费者，手动指定分区
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2020/12/10 23:54
 */
public class Consumer2 {
    public static void main(String[] args) {
        // 1 配置属性信息
        Properties properties = new Properties();
        // bootstrap.servers，服务器参数
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        // key、value反序列化
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 2 创建Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3 消费者订阅topics，手动指定消费分区，失去组管理特性
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic01", 0));
        consumer.assign(partitions);
        // 指定消费分区的位置
        // consumer.seekToBeginning(partitions);
        // 0分区，1位置
        consumer.seek(new TopicPartition("topic01", 0), 1);

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
    }
}
