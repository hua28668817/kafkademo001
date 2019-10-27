package com.rhzz.nbp.kafka01.service;

import com.csvreader.CsvWriter;
import com.rhzz.nbp.kafka01.common.Common;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
@Component
public class TestConsumer {

    String gFilePath = "D:\\work\\clickhouse\\doc\\data\\genUser2"+System.currentTimeMillis() +".csv";
    CsvWriter gCsvWriter = new CsvWriter(gFilePath, ',', Charset.forName("UTF-8"));
    int counter =0;

    /**
     * 每条记录调用写文件
     * @param list
     */
    @KafkaListener(topics = "kafka_file", id = "consumer", groupId="pc-consume",containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<String, String>> list) {
        try {
            String csvFilePath = "D:\\work\\clickhouse\\doc\\data\\genUser2"+System.currentTimeMillis() +".csv";
            CsvWriter csvWriter = new CsvWriter(csvFilePath, ',', Charset.forName("UTF-8"));
//            csvWriter.setComment(',');
            if (list==null) return;
            StringBuffer sb =new StringBuffer();
            int i=0;
            System.out.println("------------本次接收条数 Num ： "+list.size());
            for (ConsumerRecord<String, String> record : list) {
                Optional<String> kafkaMessage = Optional.ofNullable(record.value());
                // 获取消息
                if(kafkaMessage.isPresent()){
                    String[] splits = kafkaMessage.get().toString().split(",");
                    csvWriter.writeRecord(splits );

                }
            }
            csvWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @KafkaListener(topics = "kafka_file", id = "consumer2", groupId="pc-consume2", containerFactory = "batchFactory")
    public void listenBatch(List<ConsumerRecord<String, String>> list) {

        try {
           if (list==null) return;
            System.out.println("------------本次接收条数 Num ： "+list.size());

            for (ConsumerRecord<String, String> record : list) {
                Optional<String> kafkaMessage = Optional.ofNullable(record.value());
                if(kafkaMessage.isPresent()){
                    String[] splits = kafkaMessage.get().toString().split(",");
                    gCsvWriter.writeRecord(splits );
                    counter++;
                }
            }
            if(counter>=50000){ //每5万条记录写一次文件
                gCsvWriter.close();
                gFilePath = "D:\\work\\clickhouse\\doc\\data\\genUser2"+System.currentTimeMillis() +".csv";
                gCsvWriter = new CsvWriter(gFilePath, ',', Charset.forName("UTF-8"));
                counter=0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private  void writeCsv(String[] csvContent){
        // 定义一个CSV路径
        String csvFilePath = "D:\\work\\clickhouse\\doc\\data\\genUser2"+System.currentTimeMillis() +".csv";
        try {
            // 创建CSV写对象 例如:CsvWriter(文件路径，分隔符，编码格式);
            CsvWriter csvWriter = new CsvWriter(csvFilePath, ',', Charset.forName("UTF-8"));

            csvWriter.writeRecord(csvContent,true);
            csvWriter.close();
            System.out.println("--------CSV文件已经写入--------");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void receive(ConsumerRecord<?,?> record){

        Optional<?> kafkaMessage = Optional.ofNullable(record.value());

        if(kafkaMessage.isPresent()){
            Object message = kafkaMessage.get();
            System.out.println("receive message: "+message);
        }
    }
    @KafkaListener(topics = "topic_nbp2")
    public void receive2(ConsumerRecord<?,?> record){
        System.out.println("22222");
        receive(record);
    }
    @KafkaListener(topics = "topic_nbp3")
    public void receive3(ConsumerRecord<?,?> record){
        System.out.println("333333");
        receive(record);
    }
}
