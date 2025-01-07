package org.example.services;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.utils.KafkaProperties;
import static org.example.utils.UNIFORM_STRING.TOPPIC_NAME;


public class KafkaMessageService {

    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaMessageService(){
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.createKafkaPropertiesProducer();
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties.getProperties());
    }

    public void sendMessage(String message) throws ExecutionException, InterruptedException {
        System.out.println("STARTING SENDING MESSAGE TO TOPPIC: " + TOPPIC_NAME + " ,MESSAGE: " + message);
        var record = new ProducerRecord<>(TOPPIC_NAME,message,message);
        kafkaProducer.send(record, (data, ex) -> {
            if(ex != null){
                System.out.println(ex.getMessage());
            }
            System.out.println(data.topic() + ":::" + data.partition() + "/ offset " + data.offset()  + "/ timestamp " + data.timestamp() );
        }).get();
        System.out.println("FINISHING SENDING MESSAGE TO TOPPIC: " + TOPPIC_NAME + " ,MESSAGE: " + message);

    }
}
