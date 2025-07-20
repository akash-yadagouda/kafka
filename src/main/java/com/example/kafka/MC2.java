package com.example.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MC2 {
    @KafkaListener(topics = "test-topic", groupId = "my-group-2")
    public void listen(String message) {
        System.out.println("Received: " + message);
    }
}
