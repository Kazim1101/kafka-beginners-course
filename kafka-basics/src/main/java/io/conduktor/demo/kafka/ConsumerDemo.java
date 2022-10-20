package io.conduktor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args){
        log.info("I am consumer");

        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-first-consumer-group";
        String topic = "my-first-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                                                                    /*
                                                                        1- none : if no previous offset found don't even start,
                                                                        2- earliest : Read from the beginning of topic (if the msg read once that
                                                                                      will not read again because of kafka maintain that offset in
                                                                                      internal offset topic),
                                                                        3- latest : Read NOW from topic
                                                                    */
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to topic
        //consumer.subscribe(Collections.singleton(topic)); // only subscribe to one topic
        consumer.subscribe(Arrays.asList(topic));   // for one or multiple topics

        // poll for new data
        while(true){
            log.info("polling...");
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));  // 100ms to get data if now them call again with loop

            for(ConsumerRecord<String, String> record : records){
                log.info("Key : " + record.key() + ", Value : " + record.value());
                log.info("Partition : " + record.partition() + ", Offset : " + record.offset());
            }
        }

    }
}

