package com.bilgeadam.boost.apachekafka.listener;


import lombok.extern.slf4j.Slf4j;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.bilgeadam.boost.apachekafka.dto.KMessage;

@Slf4j
@Service
public class KafkaListenerService {

    @KafkaListener(
            topics = "${bilgeadam.kafka.topic}",
            groupId = "${bilgeadam.kafka.group.id}"
    )
    public void listen(@Payload KMessage message) {
        log.info("Message received.. MessageID : {} Message: {} Date : {}",
                message.getId(),
                message.getMessage(),
                message.getMessageDate());
    }
}
