package com.ywj.controller;//package com.hushuo.datacenter.controller;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RestController;
//
///**
// * @program: datacenter
// * @description: 生产者
// * @author: yang
// * @create: 2020-11-10 19:00
// */
//@RestController
//@RequestMapping("kafka")
//public class KafkaProducer {
//
//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;
//
//    @RequestMapping("send")
//    public String send(String msg){
//        System.out.println("msg="+msg);
//        kafkaTemplate.send("Demo1", msg);
//        return "success";
//    }
//
//}