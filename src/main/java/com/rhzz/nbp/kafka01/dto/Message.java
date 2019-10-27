package com.rhzz.nbp.kafka01.dto;

import lombok.Data;

import java.util.Date;

@Data
public class Message {
    private Long id;
    private Integer age;
    private String msg;
    private String sendTime;

    public  String toString(){

        return age.toString()+","+msg+","+sendTime;
    }
}
