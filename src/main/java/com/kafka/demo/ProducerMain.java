package com.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerMain {

    public static void main(String[] args) {
        Producer kafkaProd=new Producer();
        kafkaProd.sendMessage();

    }
    static class Producer {
        String newArgs[] = {"20", "rajeshtest", Util.INSTNACE.getBrokerConfig("  confluent-1")};
        int events = Integer.parseInt(newArgs[0]);
        String topic = newArgs[1];
        String brokers = newArgs[2];


 private   Properties setProperties(String brokers)
 {

     Properties props = new Properties();
     props.put("bootstrap.servers", brokers);
     props.put("client.id", "producer");
     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     return props;
 }


    void sendMessage() {
        Properties props= setProperties(brokers);
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        long timestamp= System.currentTimeMillis();
        for (int nEvents=0;nEvents<events;nEvents++) {
            String key = "Test " + nEvents;
            String msg = "Message"+nEvents;
            ProducerRecord data = new ProducerRecord<String, String>(topic, key, msg);

            //async
            //producer.send(data, (m,e) => {})
            //sync
            producer.send(data);

        }
        System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - timestamp));
        producer.close();
    }



    }

}
