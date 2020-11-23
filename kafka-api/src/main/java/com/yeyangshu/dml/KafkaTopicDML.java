package com.yeyangshu.dml;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Topic基本操作 DML管理
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2020/11/24 0:23
 */
public class KafkaTopicDML {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // 1 配置kafka属性
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");

        // 2 创建Kafka admin
        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(properties);

        // 3 创建Topic
        // 异步创建Topic
        // adminClient.createTopics(Arrays.asList(new NewTopic("topic02", 3, (short) 3)));
        // 同步创建Topic
        // CreateTopicsResult createTopic03 = adminClient.createTopics(Collections.singletonList(new NewTopic("topic03", 3, (short) 3)));
        // createTopic03.all().get();

        // 4 查看Topic列表
        ListTopicsResult topicsResult = adminClient.listTopics();
        Set<String> names = topicsResult.names().get();
        for (String name : names) {
            System.out.println(name);
        }

        // 5 查看Topic详情
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton("topic01"));
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
        for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }

        // 6 删除Topic
        // 同步删除，异步删除同 同步创建Topic
        // DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("topic02", "topic03"));
        // deleteTopicsResult.all().get();

        // 7 关闭AdminClient
        adminClient.close();
    }
}
