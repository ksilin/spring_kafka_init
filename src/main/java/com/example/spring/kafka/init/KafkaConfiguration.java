package com.example.spring.kafka.init;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;

import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.*;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.RETRIES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

//@Configuration
public class KafkaConfiguration {

    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(
                Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        RETRIES_CONFIG, 0,
                        BUFFER_MEMORY_CONFIG, 33554432,
                        KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                )
        );
    }

    //@Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProperties);
    }

    public Map<String, Object> consumerProperties = Map.of(
            BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            GROUP_ID_CONFIG, "spring-init",
            ENABLE_AUTO_COMMIT_CONFIG, true, // example says false for some reason,
            SESSION_TIMEOUT_MS_CONFIG, 33554432,
            KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    //@Bean
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        return new KafkaTemplate<Integer, String>(producerFactory());
    }


}
