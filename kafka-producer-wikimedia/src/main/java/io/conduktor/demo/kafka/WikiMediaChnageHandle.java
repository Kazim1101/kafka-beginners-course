package io.conduktor.demo.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikiMediaChnageHandle implements EventHandler {

    private final Logger log = LoggerFactory.getLogger(WikiMediaChnageHandle.class.getSimpleName());
    KafkaProducer<String, String> producer;
    String topic;

    public WikiMediaChnageHandle(KafkaProducer<String, String> producer, String topic){
        this.producer = producer;
        this.topic = topic;

    }

    @Override
    public void onOpen() throws Exception {
        // event stream is open (do nothing)
    }


    @Override
    public void onClosed() throws Exception {
        //close producer
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        // Async
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {
        // nothing
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error reading in Stream", t);
    }
}
