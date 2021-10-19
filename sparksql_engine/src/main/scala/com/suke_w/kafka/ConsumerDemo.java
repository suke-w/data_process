package com.suke_w.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/**
 * 需求：使用Java代码实现消费者代码
 * Created by xuwei
 */
public class ConsumerDemo {
    public static void main(String[] args) {
        Properties prop = new Properties();
        //指定kafak的broker地址
        prop.put("bootstrap.servers","bigdata01:9092,bigdata02:9092,bigdata03:9092");
        //指定key-value的反序列化类型
        prop.put("key.deserializer", StringDeserializer.class.getName());
        prop.put("value.deserializer", StringDeserializer.class.getName());
        //指定消费者组
        prop.put("group.id","g1");
        //开启自动提交offset功能，默认就是开启的
        prop.put("enable.auto.commit","true");
        //自动提交offset的时间间隔，单位是毫秒
        prop.put("auto.commit.interval.ms","5000");
        prop.put("auto.offset.reset","earliest");

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        Collection<String> topics = new ArrayList<String>();
        topics.add("stu_clean");
        //订阅指定的topic
        consumer.subscribe(topics);

        while(true){
            //消费数据【注意：需要修改jdk的编译级别为1.8，否则Duration.ofSeconds(1)会语法报错】
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String,String> consumerRecord : poll){
                System.out.println(consumerRecord.value());
            }
            consumer.commitSync();
        }

    }
}
