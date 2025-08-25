# Kafka POC (Proof of Concept)

This project demonstrates the usage of Apache Kafka with Spring Boot by implementing a simple library event system. It consists of a **producer** application and a **consumer** application that communicate through Kafka topics.

## Overview

- **Producer Service**: Accepts REST API requests to create or update library events and publishes these events to a Kafka topic.
- **Consumer Service**: Listens to the Kafka topic, processes library events, and persists them to a database. It also handles validation, error handling, and retry logic.

## Features

- **Spring Boot** based microservices for both producer and consumer.
- REST endpoints to publish (`POST`) and update (`PUT`) library events.
- Kafka integration for event-driven communication.
- Error handling with retries and dead-letter topics.
- Persistence using Spring Data JPA.
- Integration and unit tests for robust validation.

## Project Structure

```
kafka-poc/
│
├── library-producer/    # Producer application
│   └── src/
│       └── main/
│           └── java/com/kafkalearning/libraryproducer/
│               ├── controller/        # Exposes REST API endpoints
│               ├── producer/          # Publishes events to Kafka
│               └── domain/            # Event and domain models
│
├── library-consumer/    # Consumer application
│   └── src/
│       └── main/
│           └── java/com/learnkafka/libraryconsumer/
│               ├── consumer/          # Kafka listener
│               ├── service/           # Event processing logic
│               ├── jpa/               # Data persistence layer
│               └── entity/            # JPA entities
```

## How It Works

1. **Producer Side**:
    - Exposes REST endpoints (`/v1/libraryevent`) for creating and updating library events.
    - Serializes event objects to JSON and publishes them to the `library-events` Kafka topic.

2. **Consumer Side**:
    - Listens to the `library-events` topic.
    - Deserializes the event, validates, and saves it to the database.
    - If errors occur, retries are handled and failed records may be persisted for further analysis.

## Technologies Used

- Java
- Spring Boot
- Spring Data JPA
- Apache Kafka
- Lombok
- Embedded Kafka (for testing)
- JUnit

## Getting Started

### Prerequisites

- Java 17+
- Maven
- Docker

### Running the Applications

1. **Start Kafka and Zookeeper using Docker Compose**  
   (Ensure Docker is running on your machine)

   ```bash
   docker-compose -f docker-compose-multi-broker.yml up
   ```

2. **Start the Producer**:
    ```bash
    cd library-producer
    mvn spring-boot:run
    ```
3. **Start the Consumer**:
    ```bash
    cd library-consumer
    mvn spring-boot:run
    ```

### Example API Usage

- **Create a Library Event** (POST):
    ```
    POST /v1/libraryevent
    Content-Type: application/json

    {
      "libraryEventId": null,
      "libraryEventType": "NEW",
      "book": {
        "bookId": 123,
        "bookName": "Kafka Using Spring Boot",
        "bookAuthor": "tanish"
      }
    }
    ```

- **Update a Library Event** (PUT):
    ```
    PUT /v1/libraryevent
    Content-Type: application/json

    {
      "libraryEventId": 123,
      "libraryEventType": "UPDATE",
      "book": {
        "bookId": 456,
        "bookName": "Kafka Using Spring Boot 3.0 and latest",
        "bookAuthor": "Tanish"
      }
    }
    ```

## Testing

Both producer and consumer modules include unit and integration tests.
To run tests:
```bash
mvn test
```
