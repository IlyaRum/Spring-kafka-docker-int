package com.example.springkafkadocker.springkafkadockerint;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class KafkaProducer {

    private static final String TOPIC= "my_topic";


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void writeMessage(String msg){

        this.kafkaTemplate.send(TOPIC, "IN_KEY",  msg);
    }

    public ListenableFuture<SendResult<String, String>> sendMessage(String topic, String key, String message) {

        return this.kafkaTemplate.send(topic, key, message);
    }


}
