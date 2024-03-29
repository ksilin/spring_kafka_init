package com.example.spring.kafka.init;

import lombok.SneakyThrows;
import net.christophschubert.cp.testcontainers.CPTestContainerFactory;
import net.christophschubert.cp.testcontainers.SchemaRegistryContainer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.KafkaContainer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

import static org.awaitility.Awaitility.*;
import static java.time.Duration.*;
import static java.util.concurrent.TimeUnit.*;
import static org.hamcrest.Matchers.*;


@SpringBootTest
@DirtiesContext
//@EnableConfigurationProperties(KafkaConfiguration.class) // enables support for @ConfigurationProperties annotated beans
//@ContextConfiguration(locations = { "classpath*:application.properties" })
//@TestPropertySource("/application.properties")
//@RunWith(SpringRunner.class)
@Import(CPContainerTests.KafkaTestContainersConfiguration.class)
class CPContainerTests {

    static final CPTestContainerFactory factory = new CPTestContainerFactory();
    static final KafkaContainer kafka = factory.createKafka();
    static final SchemaRegistryContainer schemaRegistry = factory.createSchemaRegistry(kafka);

    @BeforeAll
    static public void init() {
        schemaRegistry.start(); //will implicitly start kafka
        createTopics("hobbits");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CPContainerTests.class);
    private static String kafkaTestGourp = "containerKafkaTestGourp";
    @Autowired
    private Consumer c;
    @Autowired
    private Producer p;

    @Test
    void produceAndConsumeData() throws InterruptedException {

        String bootstrapServers = kafka.getBootstrapServers();
        LOGGER.info("bootstrapServers" + bootstrapServers);

        p.generate(3);

        // TODO - how to wait until all msgs are consumed?
        LOGGER.info("msgCounter: " + c.msgCounter);

        var result = getConsumerGroups(kafkaTestGourp);
        var groups = result.join();//thenApply(groups -> groups.forEach(System.out::println));
        groups.forEach(System.out::println);
        LOGGER.info("done: " + groups);
        await().until(() -> c.msgCounter > 2);
        //assertTrue(c.msgCounter > 0);
    }

    @Test
    void noHealthCheckDefinedForCP() {
        // java.lang.RuntimeException: This container's image does not have a healthcheck declared, so health cannot be determined. Either amend the image or use another approach to determine whether containers are healthy.
        //	at org.testcontainers.containers.ContainerState.isHealthy(ContainerState.java:108)
        assertThrows(RuntimeException.class, kafka::isHealthy);
    }

    private static void createTopics(String... topics) {
        var newTopics =
                Arrays.stream(topics)
                        .map(topic -> new NewTopic(topic, 1, (short) 1))
                        .collect(Collectors.toList());
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(newTopics);
        }
    }

    private static BiFunction<Map<TopicPartition, OffsetAndMetadata>, Throwable, String> groupOffsetHandler(String s) {
        return (data, ex) -> {
            if (ex != null) {
                var failedMsg = String.format("failed to retrieve offsets for group %s with %s", s, ex);
                LOGGER.warn(failedMsg);
                return failedMsg;
            } else {
                String msg = String.format("offsets for %s: %s", s, data);
                LOGGER.info(msg);
                return msg;
            }
        };
    }

    private static <T> BiFunction<T, Throwable, String> resultToString(String description,  String name) {
        return (data, ex) -> {
            if (ex != null) {
                var failedMsg = String.format("failed to %s for %s with %s", description, name, ex);
                LOGGER.warn(failedMsg);
                return failedMsg;
            } else {
                String msg = String.format("success: %s for %s: %s", description, name, data);
                LOGGER.info(msg);
                return msg;
            }
        };
    }

    @SneakyThrows
    private static List<String> getConsumerGroups2(String... groups) {
        var gr = Arrays.asList(groups);
        printConsumerGroupsDescriptions(gr);
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            var futures = gr.stream().map((s) -> {
                        var future = admin.listConsumerGroupOffsets(s).partitionsToOffsetAndMetadata();
                        return future.toCompletionStage().toCompletableFuture().handle(resultToString("retrieve offsets for group", s));
                    }
            ).toList();
            return futures.stream().map(CompletableFuture::join).toList();
        }
    }

    @SneakyThrows
    private static CompletableFuture<List<String>> getConsumerGroups(String... groups) {
        var gr = Arrays.asList(groups);
        printConsumerGroupsDescriptions(gr);
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            var futures = gr.stream().map((s) -> {
                        var future = admin.listConsumerGroupOffsets(s).partitionsToOffsetAndMetadata();
                        return future.toCompletionStage().toCompletableFuture().handle(resultToString("retrieve offsets for group", s));
                    }
            ).toList();
            CompletableFuture<String>[] futureA = futures.toArray(CompletableFuture[]::new);
            var allDone = CompletableFuture.allOf(futureA);
            return allDone.thenApply(joinSourceFutures(futures));
        }
    }

    @NotNull
    private static <T> Function<Void, List<T>> joinSourceFutures(List<CompletableFuture<T>> futures) {
        return ignoredVoid ->
                futures.stream().map(CompletableFuture::join)
                        .collect(Collectors.toList());
    }

    private static void printConsumerGroupsDescriptions(List<String> groups) {
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            var descriptions = admin.describeConsumerGroups(groups);
            descriptions.all().toCompletionStage().toCompletableFuture().join().forEach((k, v) -> LOGGER.info("group {}: {}", k, v));
        }
    }

    @TestConfiguration
    static class KafkaTestContainersConfiguration {

        @Bean
        ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            return factory;
        }

        @Bean
        public ConsumerFactory<Integer, String> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerConfigs());
        }

        @Bean
        public Map<String, Object> consumerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaTestGourp);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return props;
        }

        @Bean
        public ProducerFactory<Integer, String> producerFactory() {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<Integer, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }

    }

}
