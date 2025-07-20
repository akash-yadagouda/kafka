package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class KafkaApplication {

    private final MessageProducer producer;

    public KafkaApplication(MessageProducer producer) {
        this.producer = producer;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @GetMapping("/send/{message}")
    public String send(@PathVariable String message) {
        producer.sendMessage(message);
        return "Sent: " + message;
    }
}
