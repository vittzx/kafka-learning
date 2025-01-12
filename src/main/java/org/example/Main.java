package org.example;

import org.example.domain.entities.Order;
import org.example.services.KafkaMessageService;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.example.utils.UNIFORM_STRING.EMAIL_TOPPIC_NAME;
import static org.example.utils.UNIFORM_STRING.FRAUDE_TOPPIC_NAME;


public class Main {

    private static final KafkaMessageService<String> emailMessageService = new KafkaMessageService<>(EMAIL_TOPPIC_NAME);
    private static final KafkaMessageService<Order> fraudMessageService = new KafkaMessageService<>(FRAUDE_TOPPIC_NAME);


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        sendMessageMain();
    }


    public static void sendMessageMain() throws ExecutionException, InterruptedException{
        System.out.println("KAFKA MESSAGE CONTROLLER START");
        for(int i =0;i < 10;i++) {

            System.out.println("STARTING MESSAGE: " + (i+1));
            String userId = UUID.randomUUID().toString();
            String orderId = UUID.randomUUID().toString();
            BigDecimal amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
            
            var order = new Order(userId, orderId, amount);

            fraudMessageService.sendMessage(userId, order);
            emailMessageService.sendMessage(userId, "order created");
        }
        System.out.println("KAFKA MESSAGE CONTROLLER FINISHED");
    }


}