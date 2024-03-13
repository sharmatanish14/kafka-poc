package com.learnkafka.libraryconsumer.jpa;

import com.learnkafka.libraryconsumer.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {
}
