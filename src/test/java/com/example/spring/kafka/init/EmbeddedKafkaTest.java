package com.example.spring.kafka.init;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import lombok.val;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
//@ExtendWith(SpringExtension.class) // TODO - what does this do exactly?
//@EnableConfigurationProperties(KafkaConfiguration.class) // enables support for @ConfigurationProperties annotated beans
//@ContextConfiguration(locations = { "classpath*:application.properties" })
//@TestPropertySource("/application.properties")
class EmbeddedKafkaTest {

	//KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

	//@Autowired
	//private ApplicationContext appCtx;

	@Autowired
	private Consumer consumer;

	@Autowired
	private Producer producer;

	@Value("${test.topic}")
	private String topic;

	@Test
	void produceSingleMessageAndSeeItConsumed() {

		final var x = "x";
		val y = "x";

		producer.generate(Integer.valueOf(3));

	//	String bootstrapServers = kafka.getBootstrapServers();
	//	System.out.println("bootstrapServers" + bootstrapServers);

		// TODO - fails with
		// org.springframework.context.ApplicationContextException: Failed to start bean 'org.springframework.kafka.config.internalKafkaListenerEndpointRegistry';
		// nested exception is org.apache.kafka.common.KafkaException: Failed to construct kafka consumer
		// Caused by: org.apache.kafka.common.config.ConfigException: No resolvable bootstrap urls given in bootstrap.servers


		//assertNotNull(appCtx.getBean(Producer.class));
	}

}
