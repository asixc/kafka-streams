package com.example.utils;

import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/*
Clase de utilidad para explorar la api AdminClient para gestionar Kafka programáticamente
 */
public class AdminUtils {
    public static String kafkaUrl = "http://localhost:9092";
    public static String purchasesTopic = "purchases";
    public static String outputTopicApple = "billed-apple";
    public static String outputTopicAndroid = "billed-andorid";
    public static String outputTopicAndroidOneOPlus = "billed-andorid-oneplus";
    public static String outputTopicOthers = "billed-others";
    static List<String> topicsToDelete = List.of(purchasesTopic, outputTopicApple, outputTopicAndroid, outputTopicAndroidOneOPlus, outputTopicOthers);

    static Logger log = LoggerFactory.getLogger(AdminUtils.class);

    public static void main(String[] args) {
        AdminUtils.overview();
    }

    public static void overview(){
        Properties config = new Properties();
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        config.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(3000));
        config.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(3000));
        AdminClient admin = AdminClient.create(config);

        ListTopicsResult topics = admin.listTopics();

        // imprimir todos los topics
        try {
            topics.names().get().stream()
                    .filter(name -> !name.startsWith("_"))
                    .forEach(name -> log.info("TOPIC NAME: {}", name));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        // comprobar si existe un topic
        DescribeTopicsResult result = admin.describeTopics(List.of(outputTopicOthers));
        try {
            TopicDescription description = result.topicNameValues().get(outputTopicOthers).get();
            System.out.println(description);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }


    public static void deleteTitlesTopic() {
        Properties config = new Properties();
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        config.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(3000));
        config.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(3000));
        AdminClient admin = AdminClient.create(config);
        try {
            admin.deleteTopics(topicsToDelete).all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        admin.close();
    }

}
