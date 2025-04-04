package com.learn.kafka.controller;


import com.learn.kafka.model.Product;
import com.learn.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka/producer")
public class ProducerController {

    @Autowired
    private KafkaService kafkaService;

    @PostMapping("/")
    public ResponseEntity<?> ProducerMessage(@RequestBody Product product) {

        boolean status = kafkaService.sendObjectMessage(product);
        if (!status) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("message", "Data not sending .. "));
        }
        return ResponseEntity.status(HttpStatus.OK).body(Map.of("message", "Data sending .. "));
    }

    @GetMapping("/{message}")
    public ResponseEntity<?> ProducerMessage(@PathVariable String message) {
        boolean status = kafkaService.sendStringMessage(message);
        if (!status) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("message", "Message not sent.."));
        }
        return ResponseEntity.status(HttpStatus.OK)
                .body(Map.of("message", "Message sent successfully."));
    }
}
