package io.conduktor.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args){
        log.info("I am producer");

        Properties properties = new Properties();

        //set producer config
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);



        for(int i=0; i <= 10; i++){

            String topic = "my-first-topic";
            String value = "Hellow from I_J _ " + i;
            String key = "id_" + i;
            // create producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);
            //send data
            //producer.send(producerRecord); // will not have any response from kafka
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        log.info("Recived new metadata for msg" + "\n" +
                                "Topic : " + metadata.topic() + "\n" +
                                "Key : " + producerRecord.key() + "\n" +
                                "Partitions : " + metadata.partition() + "\n" + // partition number where the data went in topic
                                "Offset : " + metadata.offset() + "\n" +
                                "Timestamp : " + metadata.timestamp()
                        );
                    }
                }
            });
            // end of send data

            // sticky partitioner : basically its performance improvement
            // kafka producer client uses sticky partitioner if no partition or key is given in ProducerRecord

        }

        // flush (wait send method to finish as its Async operation)
        producer.flush();
        //
        producer.close();


    }
}

