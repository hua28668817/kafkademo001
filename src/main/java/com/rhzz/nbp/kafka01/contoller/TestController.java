package com.rhzz.nbp.kafka01.contoller;

import com.rhzz.nbp.kafka01.service.TestProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private TestProducer testProducer;

    @RequestMapping("/send")
    @ResponseBody
    public String send(@RequestParam(value = "msg")String msg){
        testProducer.send(msg);
        return "发送消息成功";
    }


}
