package com.learnkafka.libraryconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LibraryConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(LibraryConsumerApplication.class, args);
	}

}
