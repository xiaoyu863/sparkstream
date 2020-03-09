package producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class CollectLog {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("metadata.broker.list", "xiaoyu1:9092,xiaoyu2:9092,xiaoyu3:9092");
        //消息传递到broker时的序列化方式
        properties.setProperty("serializer.class", StringEncoder.class.getName());
        //zk的地址
        properties.setProperty("zookeeper.connect", "xiaoyu1:2181,xiaoyu2:2181,xiaoyu3:2181");
        //是否反馈消息 0是不反馈消息 1是反馈消息
        properties.setProperty("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer(producerConfig);
        try {
            BufferedReader bf = new BufferedReader(new FileReader(new File("F:\\Program Files\\feiq\\Recv Files\\sparkcoursesinfo\\project\\rechargeanalyze\\cmcc.json")));
            String line;
            while ((line = bf.readLine()) != null) {
                KeyedMessage<String, String> keyedMessage = new KeyedMessage("recharge", line);
                Thread.sleep(2000);
                producer.send(keyedMessage);
                System.out.println(keyedMessage);
            }
            bf.close();
            producer.close();
            System.out.println("发送完毕");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
