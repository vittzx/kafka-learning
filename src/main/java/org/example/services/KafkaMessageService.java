package org.example.services;

import java.util.UUID;
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
            String key = sendMessageFraude(message);
            sendMessageEmail("Thank you! Your order [ " + message  + " ] is being processed!", key);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public String sendMessageFraude(String message) throws ExecutionException, InterruptedException {
        System.out.println("STARTING SENDING FRAUDE MESSAGE  TO TOPPIC: " + FRAUDE_TOPPIC_NAME + " ,MESSAGE: " + message);
        var key = UUID.randomUUID().toString();
        var value = key + "," + message;
        System.out.println("KEY: " + key);
        var fraudeRecord = new ProducerRecord<>(FRAUDE_TOPPIC_NAME,key, value);
        kafkaProducer.send(fraudeRecord, callback).get();
        System.out.println("FINISHING SENDING FRAUDE MESSAGE TO TOPPIC: " + FRAUDE_TOPPIC_NAME + " ,MESSAGE: " + message);
        return key;
    }


    public void sendMessageEmail(String message, String key) throws ExecutionException, InterruptedException{
        System.out.println("STARTING SENDING FRAUDE MESSAGE  TO TOPPIC: " + EMAIL_TOPPIC_NAME + " ,MESSAGE: " + message);

        var emailRecord = new ProducerRecord<>(EMAIL_TOPPIC_NAME, key,message);
        kafkaProducer.send(emailRecord, callback).get();

        System.out.println("FINISHING SENDING FRAUDE MESSAGE  TO TOPPIC: " + EMAIL_TOPPIC_NAME + " ,MESSAGE: " + message);
    }


}
