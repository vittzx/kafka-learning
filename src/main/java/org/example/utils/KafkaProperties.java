package org.example.utils;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.example.utils.UNIFORM_STRING.*;


public class KafkaProperties {

    private final Properties kafkaProperties;

    public KafkaProperties(){
        this.kafkaProperties = new Properties();
    }

    public void createKafkaPropertiesProducer(){
        this.kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_PORT);
        this.kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER_STRING_CLASS_CONFIG);
        this.kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER_GSON_CLASS_CONFIG);
    }

    public void createKafkaPropertiesConsumer(){
        this.kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_PORT);
        this.kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER_STRING_CLASS_CONFIG);
        this.kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALOZER_GSON_CLASS_CONFIG);
        this.kafkaProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
    }

    public void add_properties(Object object_propertie, Object propertie){
        this.kafkaProperties.setProperty((String) object_propertie, (String) propertie);
    }

    public void addAllProperties(Map<String, String> map){
        this.kafkaProperties.putAll(map);
    };

    public Properties getProperties(){ return kafkaProperties; }
}
