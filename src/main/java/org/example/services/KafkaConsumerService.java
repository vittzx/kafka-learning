package org.example.services;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.domain.entities.Order;
import org.example.domain.interfaces.KafkaConsumerInterface;
import org.example.utils.Gson.GsonDeserializer;
import org.example.utils.KafkaProperties;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.example.utils.UNIFORM_STRING.FRAUDE_TOPPIC_NAME;

public class KafkaConsumerService<T> implements Closeable, KafkaConsumerInterface<T> {

    private final KafkaConsumer<String, T> consumer;

    public KafkaConsumerService(Class<T> type){
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.createKafkaPropertiesConsumer();
        kafkaProperties.add_properties(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerService.class.getSimpleName());
        kafkaProperties.add_properties(ConsumerConfig.CLIENT_ID_CONFIG, KafkaConsumerService.class.getSimpleName() + "_" + UUID.randomUUID().toString());
        kafkaProperties.add_properties(GsonDeserializer.TYPE_CONFIG, type.getName());
        this.consumer = new KafkaConsumer<>(kafkaProperties.getProperties());
    }

    public KafkaConsumerService(Class<T> type, Map<String, String> properties){
        KafkaProperties kafkaProperties = new KafkaProperties();
        kafkaProperties.createKafkaPropertiesConsumer();
        kafkaProperties.add_properties(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerService.class.getSimpleName());
        kafkaProperties.add_properties(ConsumerConfig.CLIENT_ID_CONFIG, KafkaConsumerService.class.getSimpleName() + "_" + UUID.randomUUID().toString());
        kafkaProperties.add_properties(GsonDeserializer.TYPE_CONFIG, type.getName());
        kafkaProperties.addAllProperties(properties);
        this.consumer = new KafkaConsumer<>(kafkaProperties.getProperties());
    }


    @Override
    public void consumerTopic(String topicName){
        System.out.println("STARTING CONSUMER TOPIC " + topicName + " MESSAGES");
        consumer.subscribe(Collections.singletonList(topicName));

        analizeMessages();

        System.out.println("FINISHING CONSUMER TOPIC " + topicName + " MESSAGES");
    }

    public void consumerTopic(Pattern topicName){
        System.out.println("STARTING CONSUMER TOPIC " + topicName + " MESSAGES");
        consumer.subscribe(topicName);

        analizeMessages();

        System.out.println("FINISHING CONSUMER TOPIC " + topicName + " MESSAGES");
    }

    public void analizeMessages(){
        boolean condition = true;
        // int count = 0;
        while (condition) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));

            if (records.isEmpty()) {
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
        final KafkaConsumerService<Order> kafkaConsumerService = new KafkaConsumerService(Order.class);
        System.out.println("KAFKA CONSUMER FRAUDE CONTROLLER START");
        kafkaConsumerService.consumerTopic(FRAUDE_TOPPIC_NAME);
        System.out.println("KAFKA CONSUMER FRAUDE CONTROLLER FINISHED");
    }


    @Override
    public void close() {
        consumer.close();
    }
}
