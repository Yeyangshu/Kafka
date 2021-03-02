package com.yeyangshu.springbootkafka.producer;

/**
 * 消息接口
 *
 * @author yeyangshu
 * @version 1.0
 * @date 2021/3/2 23:18
 */
public interface IMessageSender {

    /**
     * 发送消息
     *
     * @param topic 主题
     * @param key 键
     * @param message 值
     */
    void sendMessage(String topic, String key, String message);

}
