//package com.qf.hz1901.day18.kafkademo;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//
//import java.util.Collections;
//import java.util.Iterator;
//import java.util.Properties;
//
///**
// * 实现Consumer，用于消费Kafka的数据并打印
// */
//public class ConsumerDemo {
//    public static void main(String[] args) {
//        Properties prop = new Properties();
//        // 指定消费的Kafka集群
//        prop.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
//        // 指定组名
//        prop.put("group.id", "ConsumerTest");
//        // 如果zookeeper没有offset值或offset值超出范围。那么就给个初始的offset
//        // earliest、lartest
//        prop.put("auto.offset.reset", "earliest");
//        // 指定key的序列化类
//        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        // 指定value的序列化类
//        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        // 实例化对象
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
//
//        // 需要先订阅topic, 可以消费多个topic的数据
//        consumer.subscribe(Collections.singletonList("test"));
//        // 消费数据
//        while (true) {
//            ConsumerRecords<String, String> poll = consumer.poll(1000);
//            Iterator<ConsumerRecord<String, String>> it = poll.iterator();
//            while (it.hasNext()) {
//                ConsumerRecord<String, String> msg = it.next();
//                System.out.println("topic:" + msg.topic());
//                System.out.println("offset:" + msg.offset());
//                System.out.println("partition:" + msg.partition());
//                System.out.println("msg:" + msg.value());
//            }
//        }
//
//
//    }
//}
