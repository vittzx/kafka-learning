package org.example.services;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.utils.KafkaProperties;

import static org.example.utils.UNIFORM_STRING.EMAIL_TOPPIC_NAME;
import static org.example.utils.UNIFORM_STRING.FRAUDE_TOPPIC_NAME;


public class KafkaMessageService {

    private final KafkaProducer<String, String> kafkaProducer;
    private Callback callback = (data, ex) -> {
        if(ex != null){
            System.out.println(ex.getMessage());
        }
        System.out.println(data.topic() + ":::" + data.partition() + "/ offset " + data.offset()  + "/ timestamp " + data.timestamp() );
    };


    public KafkaMessageService(){
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.createKafkaPropertiesProducer();
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties.getProperties());
    }


    public void sendMessage(String message){
        try {
            sendMessageFraude(message);
            sendMessageEmail("Thank you! Your order [ " + message  + " ] is being processed!");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void sendMessageFraude(String message) throws ExecutionException, InterruptedException {
        System.out.println("STARTING SENDING FRAUDE MESSAGE  TO TOPPIC: " + FRAUDE_TOPPIC_NAME + " ,MESSAGE: " + message);
        var fraudeRecord = new ProducerRecord<>(FRAUDE_TOPPIC_NAME,message,message);
        kafkaProducer.send(fraudeRecord, callback).get();
        System.out.println("FINISHING SENDING FRAUDE MESSAGE TO TOPPIC: " + FRAUDE_TOPPIC_NAME + " ,MESSAGE: " + message);
    }


    public void sendMessageEmail(String message) throws ExecutionException, InterruptedException{
        System.out.println("STARTING SENDING FRAUDE MESSAGE  TO TOPPIC: " + EMAIL_TOPPIC_NAME + " ,MESSAGE: " + message);

        var emailRecord = new ProducerRecord<>(EMAIL_TOPPIC_NAME, message,message);
        kafkaProducer.send(emailRecord, callback);

        System.out.println("FINISHING SENDING FRAUDE MESSAGE  TO TOPPIC: " + EMAIL_TOPPIC_NAME + " ,MESSAGE: " + message);
    }


}
