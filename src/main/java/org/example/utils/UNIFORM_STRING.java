package org.example.utils;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.utils.Gson.GsonDeserializer;
import org.example.utils.Gson.GsonSerializer;

public class UNIFORM_STRING {
    public static final String KAFKA_SERVER_PORT = "127.0.0.1:9092";
    public static final String KAFKA_SERIALIZER_STRING_CLASS_CONFIG = StringSerializer.class.getName();
    public static final String KAFKA_SERIALIZER_GSON_CLASS_CONFIG = GsonSerializer.class.getName();
    public static final String KAFKA_DESERIALIZER_STRING_CLASS_CONFIG = StringDeserializer.class.getName();
    public static final String KAFKA_DESERIALOZER_GSON_CLASS_CONFIG = GsonDeserializer.class.getName();
    public static final String FRAUDE_TOPPIC_NAME = "ECOMMERCE_NEW_ORDER";
    public static final String EMAIL_TOPPIC_NAME = "ECOMMERCE_SEND_EMAIL";
}
