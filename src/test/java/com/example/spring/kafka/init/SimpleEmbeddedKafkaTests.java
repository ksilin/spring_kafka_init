package com.example.spring.kafka.init;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
//@ExtendWith(SpringExtension.class) // TODO - what does this do exactly?
//@EnableConfigurationProperties(KafkaConfiguration.class) // enables support for @ConfigurationProperties annotated beans
//@ContextConfiguration(locations = { "classpath*:application.properties" })
//@TestPropertySource("/application.properties")
class SimpleEmbeddedKafkaTests {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

	@Autowired
	private Consumer c;

	@Autowired
	private Producer p;

	@Value("${test.topic}")
	private String topic;

	@Test
	void produceConsumeTest() {

		System.out.println("topic: " + topic);
		assertEquals("hobbits", topic );

		p.generate(10);

		// TODO - how to wait until all msgs are consumed?
		System.out.println("msgCounter: " + c.msgCounter);
		assertTrue(c.msgCounter > 0);
	}

}
