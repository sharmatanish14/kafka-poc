package com.learnkafka.libraryconsumer.config;

import com.learnkafka.libraryconsumer.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfig {

    private static final String RETRY = "RETRY";
    private static final String DEAD = "DEAD";

    @Autowired
    KafkaProperties properties;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @Autowired
    private FailureService failureService;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer,
            ObjectProvider<SslBundles> sslBundles) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(
                this.properties.buildConsumerProperties(sslBundles.getIfAvailable()))));
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        factory.setConcurrency(3);  // 3 means it will spin up three threads with the same instance of kafka listener
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    public DefaultErrorHandler errorHandler() {
        var fixedBackOff = new FixedBackOff(1000L, 2);
        var errorHandler = new DefaultErrorHandler(/*publishingRecoverer()*/  consumerRecordRecoverer, fixedBackOff);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.info("Failed record in Retry listener, Exception :{} , delivery attempt {} ", ex.getMessage(), deliveryAttempt);
        });

        var exceptionToIgnoreList = List.of(IllegalArgumentException.class);
//        var exceptionToRetryList = List.of(RecoverableDataAccessException.class);

//        exceptionToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
//        exceptionToRetryList.forEach(errorHandler::addRetryableExceptions);
        return errorHandler;
    }

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> {
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        });

        return recoverer;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {

        log.error("Exception in consumerRecordRecoverer :{}", e.getMessage(),e);
        if (e.getCause() instanceof RecoverableDataAccessException) {
            // recovery logic
            log.info("Inside recovery");
            failureService.saveFailedRecord((ConsumerRecord<Integer, String>) consumerRecord, e, RETRY);
        } else {
            // non-recovery logic
            log.info("Inside non-recovery");
            failureService.saveFailedRecord((ConsumerRecord<Integer, String>) consumerRecord, e, DEAD);
        }
    };

}
