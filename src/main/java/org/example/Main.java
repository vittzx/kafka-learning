package org.example;

import org.example.services.KafkaMessageService;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;


public class Main {

    private static final KafkaMessageService kafkaMessageService = new KafkaMessageService();

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        System.out.println("KAFKA MESSAGE CONTROLLER START");
        kafkaMessageService.sendMessage("PEDIDO_010992_COSSO_SPECIAL,PRODUTO_TESTE_15,RS1000.00, " + LocalDateTime.now());
        System.out.println("KAFKA MESSAGE CONTRLLER FINISH");
    }
}