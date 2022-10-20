package io.conduktor.demo.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMediaProducer {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Hello world!");

        Properties properties = new Properties();
        String bootStrapServer = "127.0.0.1:9092";
        String topic = "wikimedia.recentchange";

        //set producer config
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // it will handle the event coming it and sent to producer
        EventHandler eventHandler = new WikiMediaChnageHandle(producer, topic);
        String eventUrl = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(eventUrl));
        EventSource eventSource = builder.build();

        // start producer in other Thread
        eventSource.start();

        // we produce for 10 mins and block the program until then
        TimeUnit.MINUTES.sleep((1));



    }
}