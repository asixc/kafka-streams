package com.example;

import com.example.utils.AdminUtils;
import com.example.utils.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PurchaseFilter {

    static String appId = "stream-purchase-filter";
    static Logger log = LoggerFactory.getLogger(PurchaseFilter.class);

    public static void main(String[] args) {

        AdminUtils.deleteTitlesTopic();
        Producer.generatePurchasesTopic();


        Properties config = new Properties();
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AdminUtils.kafkaUrl);
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder builder = new StreamsBuilder();

        // consume
        KStream<String, String> purchases = builder.stream(AdminUtils.purchasesTopic);

        // processing
        KStream<String, String> purchase = purchases
                .filter((key, value) -> value.contains("Android")) // transformación 1
                .peek((key, value) -> log.info("k {} --- v {}", key, value))
                .filter((key, value) -> value.contains("OnePlus")) // transformación 2
                .peek((key, value) -> log.info("k {} --- v {}", key, value));

        // produce
        purchase.to(AdminUtils.outputTopicAndroidOneOPlus);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}
