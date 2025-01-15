        package org.example.services.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.example.services.KafkaConsumerService;

import java.util.Map;
import java.util.regex.Pattern;

import static org.example.utils.UNIFORM_STRING.KAFKA_DESERIALIZER_STRING_CLASS_CONFIG;

        public class LogService {


            public static void main(String[] args) {
                final KafkaConsumerService<String> consumer = new KafkaConsumerService<>(String.class, Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER_STRING_CLASS_CONFIG));
                consumer.consumerTopic(Pattern.compile("ECOMMERCE.*"));

            }

        }
