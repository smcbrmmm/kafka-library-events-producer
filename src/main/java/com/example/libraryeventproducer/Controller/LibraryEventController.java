package com.example.libraryeventproducer.Controller;

import com.example.libraryeventproducer.Domain.LibraryEvent;
import com.example.libraryeventproducer.Producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class LibraryEventController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        libraryEventProducer.sendLibraryEvent(libraryEvent);

        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

}
