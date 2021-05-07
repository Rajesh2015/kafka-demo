package com.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.Properties;



public class SynchronousProducer {
    private static final Logger logger = LogManager.getLogger();

    
    public static void main(String[] args) {
        RecordMetadata metadata;

        if (args.length != 2) {
            System.out.println("Please provide command line arguments: topicName and numEvents");
            System.exit(-1);
        }

        String topicName = args[0];
        int numEvents = Integer.valueOf(args[1]);
        logger.debug("topicName=" + topicName + ", numEvents=" + numEvents);

        logger.trace("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SynchronousHelloProducer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        try {
            for (int i = 1; i <= numEvents; i++) {
                metadata = producer.send(new ProducerRecord<>(topicName, i, "Simple Message-" + i)).get();
                logger.info("Message " + i + " persisted with offset " + metadata.offset()
                        + " and timestamp on " + new Timestamp(metadata.timestamp()));
            }
        } catch (Exception e) {
            logger.error("Exception occurred.");
            throw new RuntimeException(e);
        } finally {
            producer.close();
            logger.info("Finished Application - Closing Kafka Producer.");
        }

    }
}