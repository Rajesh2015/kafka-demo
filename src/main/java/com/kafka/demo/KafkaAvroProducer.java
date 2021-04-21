package com.kafka.demo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Properties;

public class KafkaAvroProducer {
    public static void main(String[] args) {
        new KafkaAvroProducer().configureKafkaAvro();
    }

    public void configureKafkaAvro() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Util.INSTNACE.getBrokerConfig("  confluent-1"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", Util.INSTNACE.getSchemaRegistryConfig());
        KafkaProducer producer = new KafkaProducer(props);


        String key = "key1";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "value1");


        ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", key, avroRecord);
        try {
            producer.send(record);
        } catch (SerializationException e) {
            // may need to do something with it
            e.printStackTrace();
        }
// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
// then close the producer to free its resources.
        finally {
            producer.flush();
            producer.close();
        }
    }
}
