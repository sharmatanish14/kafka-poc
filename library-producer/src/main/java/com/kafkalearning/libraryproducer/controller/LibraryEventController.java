package com.kafkalearning.libraryproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkalearning.libraryproducer.domain.LibraryEvent;
import com.kafkalearning.libraryproducer.producer.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@Slf4j
public class LibraryEventController {

    private final LibraryEventsProducer libraryEventsProducer;

    public LibraryEventController(LibraryEventsProducer libraryEventsProducer) {
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        log.info("Event received {} ", libraryEvent);

        libraryEventsProducer.sendLibraryEvent(libraryEvent);

        log.info("After message send");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
