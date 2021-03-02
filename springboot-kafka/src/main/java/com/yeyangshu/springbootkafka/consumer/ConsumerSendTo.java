package com.yeyangshu.springbootkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.messaging.handler.annotation.SendTo;

/**
 * 接受并转发消息
 * topic02 to topic03
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/2 22:59
 */
public class ConsumerSendTo {

    @KafkaListeners(value = {@KafkaListener(topics = {"topic02"})})
    @SendTo("topic03")
    public String listener(ConsumerRecord<String, String> record) {
        return record + "\tsend to record";
    }

}