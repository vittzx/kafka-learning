        package org.example.services.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.domain.interfaces.KafkaConsumerInterface;
import org.example.utils.KafkaProperties;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;

import static org.example.utils.UNIFORM_STRING.EMAIL_TOPPIC_NAME;

public class KafkaConsumerEmailService implements Closeable, KafkaConsumerInterface<String> {

    private final KafkaConsumer<String, String> consumer;

    public KafkaConsumerEmailService(){
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.createKafkaPropertiesConsumer();
        kafkaProperties.add_properties(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerEmailService.class.getSimpleName());
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
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2500));

            if (records.isEmpty()) {
                System.out.println("None email message found. Continues");
                continue;
            }

            System.out.println("MESSAGE FOUND");

            for (var record : records) {
                System.out.println("--------- NEW EMAIL MESSAGE ---------");
                System.out.println("MESSAGE: " + record.topic());
                System.out.println("KEY: " + record.key());
                System.out.println("VALUE: " + record.value());
                System.out.println("PARTITION: " + record.partition());
                System.out.println("OFFSET: " + record.offset());
                // count++;
                System.out.println("EMAIL SENT");
            }

            // Condição de parada
            //if (count >= 3) {
            //    condition = false;
            //}
        }
    }


    @Override
    public void close() {
        consumer.close();
    }

    public static void main(String[] args){
        final KafkaConsumerEmailService kafkaConsumerService = new KafkaConsumerEmailService();
        System.out.println("KAFKA CONSUMER EMAIL CONTROLLER START");
        kafkaConsumerService.consumerTopic(EMAIL_TOPPIC_NAME);
        System.out.println("KAFKA CONSUMER EMAIL CONTROLLER FINISHED");
    }

}
