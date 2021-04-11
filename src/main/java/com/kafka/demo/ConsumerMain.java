package com.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerMain {
    public static void main(String[] args) {
        String newArgs[] = {Util.INSTNACE.getConfig("  confluent-1"), "TestGroup","rajeshtest"};
        Consumer con = new Consumer(newArgs[0], newArgs[1], newArgs[2]);
        Properties props=con.createConsumerConfig();
        con.runConsumer(props);


    }

    static class Consumer {

        String brokers;
        String groupId;
        String topic;
        KafkaConsumer consumer;
        ExecutorService executor = null;


        public Consumer(String brokers,
                        String groupId,
                        String topic) {
            this.brokers = brokers;
            this.groupId = groupId;
            this.topic = topic;
        }




        private Properties createConsumerConfig() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            return props;
        }

        public void shutDown() {
            if (consumer != null)
                consumer.close();
            if (executor != null)
                executor.shutdown();
        }

        public void runConsumer(Properties props) {
             consumer = new KafkaConsumer<String, String>(props);

            consumer.subscribe(Collections.singletonList(topic));
            Executors.newSingleThreadExecutor().execute(() -> {
                while (true) {
                    ConsumerRecords records = consumer.poll(1000);

                    Iterator recorsIter = records.iterator();
                    while (recorsIter.hasNext()) {
                        ConsumerRecord record = (ConsumerRecord) recorsIter.next();
                        System.out.println("<Recived Message> " + record.key() + " " + record.value() + " " + record.offset());
                    }

                }


            });

        }


    }


}
