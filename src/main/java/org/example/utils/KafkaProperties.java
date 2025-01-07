package org.example.utils;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import static org.example.utils.UNIFORM_STRING.KAFKA_SERIALIZER_CLASS_CONFIG;
import static org.example.utils.UNIFORM_STRING.KAFKA_SERVER_PORT;

public class KafkaProperties {

    private final Properties kafkaProperties;

    public KafkaProperties(){
        this.kafkaProperties = new Properties();
        this.kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_PORT);
        this.kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER_CLASS_CONFIG);
        this.kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER_CLASS_CONFIG);
    }


    public void add_properties(Object object_propertie, Object propertie){
        this.kafkaProperties.setProperty((String) object_propertie, (String) propertie);
    }

    public Properties getKafkaProperties(){ return kafkaProperties; }
}
