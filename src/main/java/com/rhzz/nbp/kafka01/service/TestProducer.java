package com.rhzz.nbp.kafka01.service;


import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.rhzz.nbp.kafka01.dto.Message;
import com.rhzz.nbp.kafka01.common.Common;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Service
public class TestProducer {

    @Autowired(required = false)
    private KafkaTemplate<String,String> kafkaTemplate;
    Gson gson =new Gson();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ; //使用了默认的格式创建了一个日期格式化对象。
    public void send(String msg){
        Message message = new Message();
        message.setAge(new Random(100).nextInt());
        message.setMsg(msg);
        message.setSendTime(dateFormat.format(new Date()));

//        System.out.println("send: " + gson.toJson(message));

        kafkaTemplate.send(Common.topic_nbp, gson.toJson(message));
        sendBatch(gson.toJson(message));
    }

    private  void sendBatch(String msg){
        int num=0;
        printTime();
        Message message = new Message();
        for (int i=1;i<20;i++){

            int beginNum=i*100;
            int endNum=beginNum+10000;
            while(beginNum<endNum) {
                message.setAge(beginNum);
                message.setMsg("from kafka "+beginNum);
                message.setSendTime(dateFormat.format(new Date()));
//                String s = gson.toJson(message);
                kafkaTemplate.send("nbp_1007", message.toString());
                beginNum++;
            }
            try {
                TimeUnit.MICROSECONDS.sleep(300);

            } catch (Exception e) {
                e.printStackTrace();
            }
            printTime();
        }
        System.out.println("20万记录，发送完成");

    }

    public void printTime(){
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(currentTime);
        System.out.println(dateString);

    }

}
