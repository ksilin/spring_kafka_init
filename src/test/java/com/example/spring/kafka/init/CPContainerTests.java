package com.example.spring.kafka.init;

import net.christophschubert.cp.testcontainers.CPTestContainerFactory;
import net.christophschubert.cp.testcontainers.SchemaRegistryContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
	static public void init(){
		schemaRegistry.start(); //will implicitly start kafka
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

	@Autowired
	private Producer p;

	@Test
	void produceAndConsumeData() throws InterruptedException {

		String bootstrapServers = kafka.getBootstrapServers();
		LOGGER.info("bootstrapServers" + bootstrapServers);

		p.generate(10);

		// TODO - how to wait until all msgs are consumed?
		LOGGER.info("msgCounter: " + c.msgCounter);
		assertTrue(c.msgCounter > 0);
	}

	@Test
	void noHealthCheckDefinedForCP() {
		// java.lang.RuntimeException: This container's image does not have a healthcheck declared, so health cannot be determined. Either amend the image or use another approach to determine whether containers are healthy.
		//	at org.testcontainers.containers.ContainerState.isHealthy(ContainerState.java:108)
		assertThrows(RuntimeException.class, kafka::isHealthy);
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
