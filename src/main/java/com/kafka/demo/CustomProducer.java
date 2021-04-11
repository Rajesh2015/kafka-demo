package com.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) throws Exception{

        String topicName = "rajesh-custom-topic-java";

        Properties props = new Properties();
        props.put("bootstrap.servers", Util.INSTNACE.getConfig("  confluent-1"));
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", CustomPartitioner.class.getName());

        Producer producer = new KafkaProducer
                (props);
try {
    for (int i = 0; i < 7; i++) {
         //int v = i % 3;
        int v = i;

        producer.send(new ProducerRecord(topicName, String.valueOf(v), String.valueOf(v)));
    }
}
catch (Exception e)
{
    e.printStackTrace();
}
        System.out.println("Message sent successfully");
        producer.close();
        System.out.println("SimpleProducer Completed.");
    }

}
