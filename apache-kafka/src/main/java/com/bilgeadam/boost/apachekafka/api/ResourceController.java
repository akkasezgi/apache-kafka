package com.bilgeadam.boost.apachekafka.api;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.RequiredArgsConstructor;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bilgeadam.boost.apachekafka.dto.KMessage;

@RestController
@RequestMapping("/kmessage")
@RequiredArgsConstructor
public class ResourceController {

    @Value("${bilgeadam.kafka.topic}")
    private String topic;

    private final KafkaTemplate<String, KMessage> kafkaTemplate;

    @SuppressWarnings("unused")
	@PostMapping
    public void sendMessage(@RequestBody KMessage kMessage) throws InterruptedException, ExecutionException, TimeoutException {
        //kafkaTemplate.send(topic, UUID.randomUUID().toString(), kMessage);
        ListenableFuture<SendResult<String, KMessage>> future = 
        	      kafkaTemplate.send(topic, kMessage);
        		
        List<ListenableFuture<SendResult<String, String>>> futures = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(kMessage.getMessage().length());
        ListenableFutureCallback<SendResult<String, String>> callback =
                new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
            	System.out.println(result.getRecordMetadata().toString());
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable ex) {
                ProducerRecord<?, ?> producerRecord = ((KafkaProducerException) ex).getFailedProducerRecord();
     
                String.format("Başarısız {0} {1}"+ " " +producerRecord, ex);
                latch.countDown();
            }
        };
     
    }

   
}
