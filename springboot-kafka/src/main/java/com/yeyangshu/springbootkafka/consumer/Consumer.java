package com.yeyangshu.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;

/**
 * 最简单的接受消息
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/2 22:57
 */
public class Consumer {

    @KafkaListeners(value = {@KafkaListener(topics = {"topic01"})})
    public void listener(ConsumerRecord<String, String> record) {
        System.out.println(record);
    }

}