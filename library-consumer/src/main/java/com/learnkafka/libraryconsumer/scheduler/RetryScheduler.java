package com.learnkafka.libraryconsumer.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryconsumer.config.LibraryEventConsumerConfig;
import com.learnkafka.libraryconsumer.entity.FailureRecord;
import com.learnkafka.libraryconsumer.jpa.FailureRecordRepository;
import com.learnkafka.libraryconsumer.service.FailureService;
import com.learnkafka.libraryconsumer.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {

    private FailureRecordRepository failureRecordRepository;
    private LibraryEventService libraryEventService;

    public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventService libraryEventService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventService = libraryEventService;
    }

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {

        log.info("Retrying failed record started");
        failureRecordRepository.findAllByStatus("RETRY").forEach(failureRecord -> {

            log.info("Retrying failed record: {}", failureRecord);
            var consumerRecord = buildConsumerRecord(failureRecord);
            try {
                libraryEventService.processLibraryEvent(consumerRecord);
                failureRecord.setStatus("SUCCESS");
                failureRecordRepository.save(failureRecord);
            } catch (Exception e) {
                log.error("Exception occurred in  retryFailedRecords {} : ", e.getMessage(), e);
            }
        });

        log.info("Retrying failed record completed");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {

        return new ConsumerRecord<>(failureRecord.getTopic(), failureRecord.getPartition(), failureRecord.getOffsetValue()
                , failureRecord.getKeyValue(), failureRecord.getErrorRecord());
    }
}
