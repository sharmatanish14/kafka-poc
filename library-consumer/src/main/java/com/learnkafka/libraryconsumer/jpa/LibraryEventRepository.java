package com.learnkafka.libraryconsumer.jpa;

import com.learnkafka.libraryconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryEventRepository extends CrudRepository<LibraryEvent, Integer> {
}
