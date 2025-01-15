package org.example.utils.Gson;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    private final Gson gson = new GsonBuilder().create();

    public static final String TYPE_CONFIG = "org.example.utils.type_config";
    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String typeName =  String.valueOf(configs.get(TYPE_CONFIG));
        try {
            this.type = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Type for deserialization does not exist in the class path",e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        return gson.fromJson(new String(bytes), type);
    }
}
