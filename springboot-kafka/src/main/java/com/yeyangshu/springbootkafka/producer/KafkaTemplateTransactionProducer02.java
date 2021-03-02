package com.yeyangshu.springbootkafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 发送事务消息，使用Spring事务
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/2 23:21
 */
@Service
@Transactional(rollbackFor = Exception.class)
public class KafkaTemplateTransactionProducer02 implements IMessageSender {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void sendMessage(String topic, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        kafkaTemplate.send(record);
    }
}
