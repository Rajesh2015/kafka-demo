package com.kafka.demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import org.apache.avro.generic.GenericRecord;


import java.util.Arrays;
import java.util.Properties;

public class KafkaAvroConsumer {

    public static void main(String[] args) {

    }

    public void configureAndReceiveConsoleConsumer() {

        Properties props = new Properties();


        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Util.INSTNACE.getBrokerConfig("  confluent-1"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", Util.INSTNACE.getSchemaRegistryConfig());


        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        String topic = "navaneeth-avro";
        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Arrays.asList(topic));


        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
