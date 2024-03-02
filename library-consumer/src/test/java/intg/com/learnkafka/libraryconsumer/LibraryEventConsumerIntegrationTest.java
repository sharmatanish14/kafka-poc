package com.learnkafka.libraryconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryconsumer.consumer.LibraryEventsConsumer;
import com.learnkafka.libraryconsumer.entity.Book;
import com.learnkafka.libraryconsumer.entity.LibraryEvent;
import com.learnkafka.libraryconsumer.entity.LibraryEventType;
import com.learnkafka.libraryconsumer.jpa.LibraryEventRepository;
import com.learnkafka.libraryconsumer.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = "library-events", partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventService libraryEventService;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumer;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    ObjectMapper objectMapper;


    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

        // given
        String jsonData = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"libraryEventType\": \"NEW\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot 3.0\",\n" +
                "        \"bookAuthor\": \"Tanish book\"\n" +
                "    }\n" +
                "}";

        kafkaTemplate.sendDefault(jsonData).get();

        // when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventRepository.findAll();
        assert libraryEventList.size() == 1;

        libraryEventList.stream().forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(456, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        String jsonData = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"libraryEventType\": \"NEW\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot 3.0\",\n" +
                "        \"bookAuthor\": \"Tanish book\"\n" +
                "    }\n" +
                "}";

        LibraryEvent libraryEvent = objectMapper.readValue(jsonData, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);

        // publish the update library event

        Book updatedBook = Book.builder().bookId(456).bookName("Kafka Using Spring Boot 3.0 and latest").bookAuthor("Tanish").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);

        String updatedJsonData = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJsonData).get();

        // when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 3.0 and latest", persistedLibraryEvent.getBook().getBookName());

    }

    @Test
    void publishUpdatedLibraryEventWithInvalidLibraryEventId() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        String jsonData = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"libraryEventType\": \"UPDATE\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka Using Spring Boot 3.0\",\n" +
                "        \"bookAuthor\": \"Tanish book\"\n" +
                "    }\n" +
                "}";

        kafkaTemplate.sendDefault(jsonData).get();

        // when
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumer, times(6)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventService, times(6)).processLibraryEvent(isA(ConsumerRecord.class));


    }

}
