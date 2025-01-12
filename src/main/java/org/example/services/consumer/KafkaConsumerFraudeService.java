package org.example.services.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.domain.interfaces.KafkaConsumerInterface;
import org.example.utils.KafkaProperties;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

import static org.example.utils.UNIFORM_STRING.FRAUDE_TOPPIC_NAME;

public class KafkaConsumerFraudeService implements Closeable, KafkaConsumerInterface<String> {

    private final KafkaConsumer<String, String> consumer;

    public KafkaConsumerFraudeService(){
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.createKafkaPropertiesConsumer();
        kafkaProperties.add_properties(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerFraudeService.class.getSimpleName());
        kafkaProperties.add_properties(ConsumerConfig.CLIENT_ID_CONFIG, KafkaConsumerFraudeService.class.getSimpleName() + "_" + UUID.randomUUID().toString());
        this.consumer = new KafkaConsumer<>(kafkaProperties.getProperties());
    }

    public void consumerTopic(String topicName){
        System.out.println("STARTING CONSUMER TOPIC " + topicName + " MESSAGES");
        consumer.subscribe(Collections.singletonList(topicName));

        analizeMessages();

        System.out.println("FINISHING CONSUMER TOPIC " + topicName + " MESSAGES");
    }

    public void analizeMessages(){
        boolean condition = true;
        // int count = 0;
        while (condition) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            if (records.isEmpty()) {
                System.out.println("None message found. Continues");
                continue;
            }

            System.out.println("MESSAGE FOUND");

            for (var record : records) {
                System.out.println("--------- NEW MESSAGE ---------");
                System.out.println("MESSAGE: " + record.topic());
                System.out.println("KEY: " + record.key());
                System.out.println("VALUE: " + record.value());
                System.out.println("PARTITION: " + record.partition());
                System.out.println("OFFSET: " + record.offset());
            }

            // Condição de parada
            // if (count >= 3) {
            //    condition = false;
            // }
        }
    }



    public static void main(String[] args){
        final KafkaConsumerFraudeService kafkaConsumerService = new KafkaConsumerFraudeService();
        System.out.println("KAFKA CONSUMER FRAUDE CONTROLLER START");
        kafkaConsumerService.consumerTopic(FRAUDE_TOPPIC_NAME);
        System.out.println("KAFKA CONSUMER FRAUDE CONTROLLER FINISHED");
    }

    @Override
    public void close() {
        consumer.close();
    }
}
