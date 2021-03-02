package com.yeyangshu.springbootkafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * KafkaTemplate发送消息，非事务
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/2 23:04
 */
public class KafkaTemplateProducer {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 发送消息
     */
    public void sendMessage() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "key1", "yeyangshu");
        kafkaTemplate.send(record);
    }
}
