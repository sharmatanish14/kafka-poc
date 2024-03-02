package com.learnkafka.libraryconsumer.entity;

import jakarta.persistence.*;
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Builder
public class LibraryEvent {

    @Id
    @GeneratedValue
    Integer libraryEventId;
    @Enumerated(EnumType.STRING)
    LibraryEventType libraryEventType;
    @OneToOne(mappedBy = "libraryEvent", cascade = CascadeType.ALL)
    @ToString.Exclude
    Book book;

}
