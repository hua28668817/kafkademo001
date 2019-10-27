package com.rhzz.nbp.kafka01.service.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CustomProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        //1.创建1个生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //2.调用send方法
        for (int i = 0; i < 100000; i++) {

            try {
                producer.send(new ProducerRecord<String, String>("nbp_1007", i + "", i+" , message-" + i+", 2019-10-07 16:20:30"));

                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //3.关闭生产者
        producer.close();
    }
}
