package com.learnkafka.libraryconsumer.service;

import com.learnkafka.libraryconsumer.entity.FailureRecord;
import com.learnkafka.libraryconsumer.jpa.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {

    @Autowired
    FailureRecordRepository failureRecordRepository;


    public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String status) {

        FailureRecord failureRecord = new FailureRecord(null, consumerRecord.topic(), consumerRecord.key(), consumerRecord.value(), consumerRecord.partition()
                , consumerRecord.offset(), e.getCause().getMessage(), status);

        failureRecordRepository.save(failureRecord);

    }
}
