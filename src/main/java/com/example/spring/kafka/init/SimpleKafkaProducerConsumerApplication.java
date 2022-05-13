package com.example.spring.kafka.init;

import com.github.javafaker.Faker;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SpringBootApplication
public class SimpleKafkaProducerConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SimpleKafkaProducerConsumerApplication.class, args);
	}
}

@RequiredArgsConstructor // Lombok generates the Ctor
@Component
class Producer {
	@Autowired
	private final KafkaTemplate<Integer, String> template;

	Faker faker;

	// wired to applciation starting in example, I prefer these things to be explicitly called
	// @EventListener(ApplicationStartedEvent.class)
	public void generate(Integer limit){
		Optional<Integer> maybeLimit = Optional.ofNullable(limit);
		Integer a = maybeLimit.orElse(Integer.MAX_VALUE);
		faker = Faker.instance();
		// from reactor core
		final Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
		final Flux<String> quotes = Flux.fromStream(Stream.generate(new Supplier<>() {
			@Override
			public String get() {
				return faker.hobbit().quote();
			}
		}));
		Function<Tuple2<Long, String>, Object> mapper = objects -> {
			System.out.println("producing " + objects.getT2());
			return template.send("hobbits", faker.random().nextInt(42), objects.getT2());
		};
		Flux.zip(interval, quotes).map(mapper).take(a).blockLast();
	}
}

@RequiredArgsConstructor
@Getter
@Component
class Consumer {

	static final Function<String, Object> noopProcessor = str -> str;

	int msgCounter = 0;

	// TODO - there is much more to explore with this annotation
	@KafkaListener(topics = {"hobbits"}, groupId = "spring-init")
	public void consume(String quote) { // Function<String, Object> processor ){
		System.out.println("received = " + quote);
		msgCounter = msgCounter + 1;
	}

	@KafkaListener(topics = {"hobbits"}, groupId = "spring-init-two")//, containerFactory = "kafkaManualAckListenerContainerFactory")
	public void consume(ConsumerRecord<Integer, String> quote){
		System.out.println("received 2 = " + quote);
	}

	@KafkaListener(topics = {"hobbits"}, groupId = "spring-init-three")
	public void consume(ConsumerRecord<Integer, String> quote,
						@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
						@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts){
		System.out.println("received 3 = " + quote + "topic: " + topic + "partition: " + partition + "ts: " + ts);
	}
}
