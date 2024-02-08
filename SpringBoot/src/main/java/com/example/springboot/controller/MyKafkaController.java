package com.example.springboot.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/msg")
public class MyKafkaController {

    private final static String TOPIC_NAME = "my-replicated-topic";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/send")
    public String sendMsg() {
        kafkaTemplate.send(TOPIC_NAME, 0, "key", "This is a message!");
        return "Send success.";
    }
}
