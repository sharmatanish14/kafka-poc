package com.kafkalearning.libraryproducer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearning.TestUtil;
import com.kafkalearning.libraryproducer.domain.LibraryEvent;
import com.kafkalearning.libraryproducer.producer.LibraryEventsProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventController.class)
class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {

        when(libraryEventsProducer.sendLibraryEvent(isA(LibraryEvent.class))).thenReturn(null);

        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());
        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());


    }

    @Test
    void postLibraryEventWithInvalidValues() throws Exception {

        when(libraryEventsProducer.sendLibraryEvent(isA(LibraryEvent.class))).thenReturn(null);

        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());
        String expectedErrorMessage = "book.bookId - must not be null, book.bookName - must not be blank";

        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));

    }
}