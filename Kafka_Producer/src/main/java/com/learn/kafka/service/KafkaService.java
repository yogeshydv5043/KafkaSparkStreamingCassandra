package com.learn.kafka.service;

import com.learn.kafka.constants.AppConstant;
import com.learn.kafka.model.Product;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.UUID;


@Service
public class KafkaService {

    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);
    @Autowired
    private KafkaTemplate<String, Object> jsonTemplate;

    @Autowired
    private KafkaTemplate<String, String> stringTemplate;

    public boolean sendObjectMessage(Product product) {
        product.setId(UUID.randomUUID());
        product.setCreated_at(LocalDate.now().toString());
        jsonTemplate.send(AppConstant.OBJECT_TOPIC, product);
        log.info("Data Sending : {}", product);
        return true;
    }

    public boolean sendStringMessage(String message) {
        if (message != null) {
            stringTemplate.send(AppConstant.STRING_TOPIC, message);
            log.info("String Data Sending : {}", message);
            return true;
        }
        return false;
    }
}