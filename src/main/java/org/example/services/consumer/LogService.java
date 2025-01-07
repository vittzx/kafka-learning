        package org.example.services.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.utils.KafkaProperties;

import java.time.Duration;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.example.utils.UNIFORM_STRING.EMAIL_TOPPIC_NAME;

        public class LogService {

            private final KafkaConsumer<String, String> consumer;

            public LogService(){
                KafkaProperties kafkaProperties = new KafkaProperties();
                kafkaProperties.createKafkaPropertiesConsumer();
                kafkaProperties.add_properties(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
                this.consumer = new KafkaConsumer<>(kafkaProperties.getProperties());
            }

            public void consumerTopic(){
                System.out.println("STARTING CONSUMER TOPIC ALL ECOMMERCE TOPICS MESSAGES");
                consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

                analizeMessages();

                System.out.println("STARTING CONSUMER TOPIC ALL ECOMMERCE TOPICS MESSAGES");
            }

            private void analizeMessages(){
                boolean condition = true;
                while (condition) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2500));

                    if (records.isEmpty()) {
                        System.out.println("None log message found. Continues");
                        continue;
                    }

                    System.out.println("MESSAGE FOUND");

                    for (var record : records) {
                        System.out.println("--------- NEW LOG MESSAGE ---------");
                        System.out.println("MESSAGE: " + record.topic());
                        System.out.println("KEY: " + record.key());
                        System.out.println("VALUE: " + record.value());
                        System.out.println("PARTITION: " + record.partition());
                        System.out.println("OFFSET: " + record.offset());
                        System.out.println("LOG ");
                    }

                    // Condição de parada
                    //if (count >= 3) {
                    //     condition = false;
                    //}
                }
            }


            public static void main(String[] args){
                final LogService kafkaConsumerService = new LogService();
                System.out.println("KAFKA CONSUMER LOG CONTROLLER START");
                kafkaConsumerService.consumerTopic();
                System.out.println("KAFKA CONSUMER LOG CONTROLLER FINISHED");
            }

        }
