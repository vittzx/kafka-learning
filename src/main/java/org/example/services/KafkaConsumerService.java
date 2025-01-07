package org.example.services;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.utils.KafkaProperties;

import java.time.Duration;
import java.util.Collections;

import static org.example.utils.UNIFORM_STRING.TOPPIC_NAME;

public class KafkaConsumerService {

    private final KafkaConsumer<String, String> consumer;

    public KafkaConsumerService(){
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.createKafkaPropertiesConsumer();
        kafkaProperties.add_properties(ConsumerConfig.GROUP_ID_CONFIG, KafkaMessageService.class.getSimpleName());
        this.consumer = new KafkaConsumer<>(kafkaProperties.getProperties());
    }

    public void consumerTopic(String topicName){
        System.out.println("STARTING CONSUMER TOPIC " + topicName + " MESSAGES");

        consumer.subscribe(Collections.singletonList(topicName));
        boolean condition = true;

        while(condition) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            if (records.isEmpty()) {
                System.out.println("None message found. Continues");
                continue;
            }

            System.out.println("MESSAGE FOUND");

            for(var record: records){
                System.out.println("--------- NEW MESSAGE ---------");
                System.out.println("MESSAGE: " + record.topic());
                System.out.println("KEY: " + record.key());
                System.out.println("VALUE: " + record.value());
                System.out.println("PARTITION: " + record.partition());
                System.out.println("OFFSET: " + record.offset());
            }



            if(records.count() == 3){ condition = false; }
        }
        System.out.println("FINISHING CONSUMER TOPIC " + topicName + " MESSAGES");
    }

    public static void main(String[] args){
        final KafkaConsumerService kafkaConsumerService = new KafkaConsumerService();
        System.out.println("KAFKA CONSUMER CONTROLLER START");
        kafkaConsumerService.consumerTopic(TOPPIC_NAME);
        System.out.println("KAFKA CONSUMER CONTROLLER FINISHED");
    }


}
