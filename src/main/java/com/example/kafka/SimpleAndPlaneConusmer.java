package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleAndPlaneConusmer {

    public static void main(String[] args) {

        // Step 1: Create consumer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address
        props.put("group.id", "my-java-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // Or "latest"

        // Step 2: Create KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Step 3: Subscribe to topic(s)
        consumer.subscribe(Collections.singletonList("test-topic")); // Replace with your topic name

        // Step 4: Poll loop
        try {
            System.out.println("Starting Kafka consumer...");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close(); // Always close the consumer
        }
    }
}
