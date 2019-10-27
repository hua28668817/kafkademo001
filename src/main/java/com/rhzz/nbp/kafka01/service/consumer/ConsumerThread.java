package com.rhzz.nbp.kafka01.service.consumer;

import com.rhzz.nbp.kafka01.common.Common;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.annotation.KafkaListener;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

public class ConsumerThread extends Thread{
    KafkaConsumer<String, String> consumer;
    String topic;

    public ConsumerThread(  String topic) {
        this.topic = topic;
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上bootstrap-servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        // 制定consumer group
        props.put("group.id", "bosh");

        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 实例化对象
        consumer = new KafkaConsumer(props); //一个新的group的消费者去消费一个topic
      //  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest"); //这个属性. 它能够消费昨天发布的数据

    }
    @Override
    public void run() {
        consumer.subscribe(Collections.singleton(this.topic));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
             //  ConsumerRecords<String, String> records = consumer.poll(new Duration(100L));
            System.out.println("本次收到消息 个数："+records.count());
            records.forEach(record->{
                //null->gp kafka practice msg:0->63
                System.out.println("->"+record.value()+"->"+Thread.currentThread().getName());
            });
        }
    }

    public static void main(String[] args) {
        new ConsumerThread(Common.topic_nbp).start();
    }
}
