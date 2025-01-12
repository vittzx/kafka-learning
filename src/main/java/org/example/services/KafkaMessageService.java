package org.example.services;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.domain.entities.Order;
import org.example.utils.KafkaProperties;

import static org.example.utils.UNIFORM_STRING.EMAIL_TOPPIC_NAME;
import static org.example.utils.UNIFORM_STRING.FRAUDE_TOPPIC_NAME;


public class KafkaMessageService<T> {

    private final KafkaProducer<String, T> kafkaProducer;
    private final String TOPPIC_NAME;
    private Callback callback = (data, ex) -> {
        if(ex != null){
            System.out.println(ex.getMessage());
        }
        System.out.println(data.topic() + ":::" + data.partition() + "/ offset " + data.offset()  + "/ timestamp " + data.timestamp() );
    };


    public KafkaMessageService(String topicName){
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.createKafkaPropertiesProducer();
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties.getProperties());
        this.TOPPIC_NAME = topicName;
    }


    public void sendMessage(String key, T message) throws ExecutionException, InterruptedException{
        System.out.println("STARTING SENDING MESSAGE  TO TOPPIC: " + FRAUDE_TOPPIC_NAME + " ,MESSAGE: " + message.toString());
        System.out.println("KEY: " + key);
        var record = new ProducerRecord<>(TOPPIC_NAME,key, message);
        kafkaProducer.send(record, callback).get();
        System.out.println("FINISHING SENDING MESSAGE TO TOPPIC: " + FRAUDE_TOPPIC_NAME + " ,MESSAGE: " + message.toString());
    }

}
