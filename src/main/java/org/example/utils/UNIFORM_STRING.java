package org.example.utils;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class UNIFORM_STRING {
    public static final String KAFKA_SERVER_PORT = "127.0.0.1:9092";
    public static final String KAFKA_SERIALIZER_CLASS_CONFIG = StringSerializer.class.getName();
    public static final String KAFKA_DESERIALIZER_CLASS_CONFIG = StringDeserializer.class.getName();
    public static final String TOPPIC_NAME = "ECOMMERCE_NEW_ORDER";
}
