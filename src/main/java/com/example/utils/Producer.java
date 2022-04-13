package com.example.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class Producer {

    static String kafkaUrl = "http://localhost:9092";

    public static void generatePurchasesTopic() {
        Properties config = new Properties();
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        List.of(
            "purchase iphone Apple 1460.30",
            "purchase OnePlus Android 909.09",
            "purchase Motorola Android 101.50",
            "purchase iphone Apple 1460.30",
            "purchase OnePlus Android 909.09",
            "purchase Xiaomi Linux 101.99",
            "purchase OnePlusNord Android 303.09")
                .stream()
                .forEach(
                   msg -> producer.send(new ProducerRecord<>(AdminUtils.purchasesTopic, msg.split(" ")[2], msg))
                );


    }


}
