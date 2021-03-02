package com.yeyangshu.springbootkafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 发送事务消息
 * 配置文件中需要开启spring.kafka.producer.transaction-id-prefix=transaction-id-
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/2 23:11
 */
public class KafkaTemplateTransactionProducer {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 发送事务消息
     */
    public void sendTransactionMessage() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic01", "key1", "yeyangshu");
        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback() {
            @Override
            public Object doInOperations(KafkaOperations kafkaOperations) {
                kafkaOperations.send(record);
                return null;
            }
        });
    }
}
