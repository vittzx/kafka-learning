package org.example.domain.interfaces;

public interface KafkaConsumerInterface<T> {

    void analizeMessages();
    void consumerTopic(String message);

}
