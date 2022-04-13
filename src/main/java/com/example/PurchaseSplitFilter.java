package com.example;

import com.example.utils.AdminUtils;
import com.example.utils.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PurchaseSplitFilter {

    static Logger log = LoggerFactory.getLogger(PurchaseSplitFilter.class);
    static String appId = "demo-streams-mobiles-filter";

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
        KStream<String, String> titles = builder.stream(AdminUtils.purchasesTopic);

        // processing and produce
        titles.split()
                .branch((key, value) -> value.toLowerCase().contains("apple"), Branched.withConsumer(kstream -> kstream.to(AdminUtils.outputTopicApple)))
                .branch((key, value) -> value.toLowerCase().contains("android"), Branched.withConsumer(kstream -> kstream.to(AdminUtils.outputTopicAndroid)))
                .defaultBranch(Branched.withConsumer(kstream -> kstream.to(AdminUtils.outputTopicOthers)));


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}


