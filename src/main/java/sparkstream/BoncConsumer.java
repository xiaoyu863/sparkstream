package sparkstream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class BoncConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092,broker2:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        String topic = "customerContacts";

        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("Reading topic:" + topic);

        while (true) {
            // 这里使用之前生产者使用的Avro生成的Customer类
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic() + "," + record.partition() + "," + record.offset() + " , "
                        + record.key() + "," + record.value());
            }
        }
    }
}
