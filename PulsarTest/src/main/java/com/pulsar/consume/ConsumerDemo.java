package com.pulsar.consume;

import org.apache.pulsar.client.api.*;

import java.util.concurrent.TimeUnit;
/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-05-11 18:07
 */
public class ConsumerDemo {

    //Pulsar集群中broker的serviceurl
    private static final String brokerServiceurl = "pulsar://hadoop101:6650";
    //需要订阅的topic name
    private static final String topicName = "persistent://public/default/my-topic";
    //订阅名
    private static final String subscriptionName = "my-sub";

    public static void main(String[] args) throws PulsarClientException {
        //构造Pulsar client
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerServiceurl)
                .build();

        //创建consumer
        Consumer consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)//指定消费模式，包含：Exclusive，Failover，Shared，Key_Shared。默认Exclusive模式
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)//指定从哪里开始消费，还有Latest，valueof可选，默认Latest
                .negativeAckRedeliveryDelay(60, TimeUnit.SECONDS)//指定消费失败后延迟多久broker重新发送消息给consumer，默认60s
                .subscribe();

        //消费消息
        while (true) {
            Message message = consumer.receive();

            try {
                System.out.printf("Message received: %s%n", new String(message.getData()));
                consumer.acknowledge(message);
            } catch (Exception e) {
                e.printStackTrace();
                consumer.negativeAcknowledge(message);
            }

        }
    }


}