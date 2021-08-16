package com.ywj.service.kafka;//package com.hushuo.datacenter.service.kafka;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Component;
//
///**
// * @program: datacenter
// * @description: kafka测试
// * @author: yang
// * @create: 2020-11-10 16:08
// */
//@Component
//public class KafkaConsumer {
//
//    @KafkaListener(topics = "Demo1")
//    public void listen (ConsumerRecord<?, ?> record) throws Exception {
//        System.out.printf("topic = %s, offset = %d, value = %s \n", record.topic(), record.offset(), record.value());
//    }
//
//}