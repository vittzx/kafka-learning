package org.example;

import org.example.services.KafkaConsumerService;
import org.example.services.KafkaMessageService;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;


public class Main {

    private static final KafkaMessageService kafkaMessageService = new KafkaMessageService();
    private static final KafkaConsumerService kafkaConsumerService = new KafkaConsumerService();


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        sendMessageMain();
    }


    public static void sendMessageMain() throws ExecutionException, InterruptedException{
        System.out.println("KAFKA MESSAGE CONTROLLER START");
        kafkaMessageService.sendMessage("MESAGE_007,PRODUTO_TESTE_15,RS1000.00, " + LocalDateTime.now());
        System.out.println("KAFKA MESSAGE CONTROLLER FINISHED");
    }


}