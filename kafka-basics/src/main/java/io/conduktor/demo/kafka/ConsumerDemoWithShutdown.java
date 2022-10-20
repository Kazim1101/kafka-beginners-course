package io.conduktor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    // Graceful shutdown
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static void main(String[] args){
        log.info("I am consumer");

        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-second-consumer-group"; // mostly an application associated with an GroupId
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

        // Get reference to the current thread as the shutdown will happen in other thread
        final Thread mainThread = Thread.currentThread();

        // adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {  // this thread will run will we will shut down our consumer/application
                log.info("Detected a shutdown, let execute the consumer by calling consumer.wakeup().. :D");
                consumer.wakeup();  // once this method called it will tell consumer to run+poll but throw a wakeup execution

                // join the main thread to allow the execution of code in main thread
                try {
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe consumer to topic
            //consumer.subscribe(Collections.singleton(topic)); // only subscribe to one topic
            consumer.subscribe(Arrays.asList(topic));   // for one or multiple topics

            // poll for new data
            while(true){
                //log.info("polling...");


                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));  // 100ms to get data if now them call again with loop

                for(ConsumerRecord<String, String> record : records){
                    log.info("Key : " + record.key() + ", Value : " + record.value());
                    log.info("Partition : " + record.partition() + ", Offset : " + record.offset());
                }

            }
        }catch (WakeupException e){
            log.info("Wakeup execution!");
            // ignore it as this execution is aspect on exiting of consumer
        } catch (Exception e){
            log.error("Unexpected Execution " + e);
        } finally {
            consumer.close(); // this will also close the offsets if need be
            // if we don't do the gracefully shut down then we will exit the program with exit code other than 1 e.g . exit code 13
            log.info("Consumer is Graceful shutdown");
        }

    }
}

