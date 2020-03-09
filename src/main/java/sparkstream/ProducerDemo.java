package sparkstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 模拟生产者，将数据不断地发送到Kafka
 */
public class ProducerDemo {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        // 指定Kafka的集群列表
        prop.put("bootstrap.servers", "xiaoyu1:9092,xiaoyu2:9092,xiaoyu3:9092");
        // 对Producer响应的方式
        prop.put("request.required.acks", "1");
        // key的序列化类
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化类
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "test";

        // 实例化对象, 其中key用于存储offset，可以为空。value用于存储数据。
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        int i = 0;
        while (i <= 10000) {
            String msg = i + "hello huahua";
            producer.send(new ProducerRecord<String, String>(topic, msg));
            i ++;
            Thread.sleep(500);
        }

    }
}
