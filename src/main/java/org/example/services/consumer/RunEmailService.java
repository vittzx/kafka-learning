package org.example.services.consumer;

import static org.example.utils.UNIFORM_STRING.EMAIL_TOPPIC_NAME;

public class RunEmailService {


    public static void main(String[] args) {
        final KafkaConsumerService<String> consumerEmail = new KafkaConsumerService<>(String.class);
        System.out.println("START EMAIL CONSUMER CONTROLLER ");
        consumerEmail.consumerTopic(EMAIL_TOPPIC_NAME);
        System.out.println("FINISH EMAIL CONSUMER CONTROLLER ");
        consumerEmail.close();
    }
}
