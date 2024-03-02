package com.learnkafka.libraryconsumer.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Builder
public class Book {
    @Id
    Integer bookId;
    String bookName;
    String bookAuthor;
    @OneToOne
    @JoinColumn(name = "libraryEventId")
    LibraryEvent libraryEvent;
}
