## NOTES

* test with TestContainers does not wait for Kafka to start

Fails the initialization of the configuration bean.

`java.lang.IllegalStateException: Mapped port can only be obtained after the container is started`

Have tried to enforce waiting by modifying the `@ClassRule`, using:

`.waitingFor(Wait.forListeningPort());`
`.waitingFor(Wait.forHealthcheck());`
`.waitingFor(Wait.forLogMessage(".*started.*", 1)`

also by adding an initial startup 

`.withStartupTimeout(Duration.ofSeconds(180));`

also with explicit waiting in the test method

```java
		while(!kafka.isHealthy()){
			Thread.sleep(1000);
			System.out.println("not healthy yet, waiting a bit");
		}
```

* TODO - try with explicitly set port, like in example

https://exceptionly.com/2022/04/09/database-and-kafka-integration-testcontainers-spring-boot/

would REALLY like to avoid

taken from example: 

https://www.baeldung.com/spring-boot-kafka-testing
https://github.com/eugenp/tutorials/blob/master/spring-kafka/src/test/java/com/baeldung/kafka/testcontainers/KafkaTestContainersLiveTest.java

## references

* https://developer.confluent.io/get-started/spring-boot/#introduction
* https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java-springboot.html
* https://www.testcontainers.org/modules/kafka/
* https://www.testcontainers.org/features/startup_and_waits/#startup-check-strategies
* WaitStrategy: https://www.javadoc.io/doc/org.testcontainers/testcontainers/latest/org/testcontainers/containers/wait/strategy/WaitStrategy.html

## overriding properties


* `application.properties` can also be `application.yml`