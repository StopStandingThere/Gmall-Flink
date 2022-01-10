package com.szl.gmall.logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@Slf4j
//@RestController  = @Controller+@ResponseBody
//表示返回普通对象而不是页面
@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("test")
    public String test(@RequestParam("name") String name, @RequestParam("age") int age){
        System.out.println("success");
        return "name: "+name+" age: "+age;
    }


    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){

//        Logger logger = LoggerFactory.getLogger(LoggerController.class);
//        logger.info(jsonStr);

        //打印数据到控制台并落盘
        log.info(jsonStr);

        //将数据写入Kafka
        kafkaTemplate.send("ods_base_log",jsonStr);

        return "success";
    }
}
