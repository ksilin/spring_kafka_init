package com.example.spring.kafka.init;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@DirtiesContext
@ContextConfiguration(
		initializers = SimpleContainerKafkaTests.KafkaTestContainersConfiguration.class)
//@EnableConfigurationProperties(KafkaConfiguration.class) // enables support for @ConfigurationProperties annotated beans
//@ContextConfiguration(locations = { "classpath*:application.properties" })
//@TestPropertySource("/application.properties")
//@RunWith(SpringRunner.class)
@Import(SimpleContainerKafkaTests.KafkaTestContainersConfiguration.class)
class SimpleContainerKafkaTests {

	@ClassRule
	public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
			.waitingFor(Wait.forListeningPort())
			//.waitingFor(Wait.forHealthcheck());
			//.waitingFor(Wait.forLogMessage(".*started.*", 1)
			.withStartupTimeout(Duration.ofSeconds(180));

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

	@Autowired
	private Consumer c;

	@Autowired
	private Producer p;

	//@Value("${test.topic}")
	//private String topic;

	@Test
	void produceAndConsumeData() throws InterruptedException {
//		while(!kafka.isHealthy()){
//			Thread.sleep(1000);
//			System.out.println("not healthy yet, waiting a bit");
//
//		}
//		String bootstrapServers = kafka.getBootstrapServers();
//		System.out.println("bootstrapServers" + bootstrapServers);

		//System.out.println("topic: " + topic);
		//assertEquals("hobbits", topic );

		p.generate(3);

		// TODO - how to wait until all msgs are consumed?
		System.out.println("msgCounter: " + c.msgCounter);
		await().until(() ->c.msgCounter > 2);
		//assertTrue(c.msgCounter > 0);
	}

	@TestConfiguration
	static class KafkaTestContainersConfiguration {

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
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
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "containerKafkaTestGourp");
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
