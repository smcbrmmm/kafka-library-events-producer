package com.example.libraryeventproducer.Producer;

import com.example.libraryeventproducer.Domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    String topics = "library-events";

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        int key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = kafkaTemplate.sendDefault(key, value);
        callBack(key, value, sendResultListenableFuture);
    }

    public void sendLibraryEvent_approach3(LibraryEvent libraryEvent) throws JsonProcessingException {

        int key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord =  buildProducerRecord(key, value, topics);
        ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = kafkaTemplate.send(topics ,key, value);
        callBack(key, value, sendResultListenableFuture);
    }

    private void callBack(int key, String value, ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture) {
        sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    public ProducerRecord<Integer, String> buildProducerRecord(int key, String value, String topics){

        // add header for publish the message to consumer
        List<Header> recordHeaders = List.of(new RecordHeader("event-source","scanner".getBytes()));

        return new ProducerRecord<>(topics, null, key ,value, recordHeaders);
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        int key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult;

        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get();
        } catch (InterruptedException | ExecutionException ex ) {
            log.error("Error sending the Message and the exception is {}", ex.getMessage());
            throw ex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return sendResult;
    }

    public void handleSuccess(int key, String value, SendResult<Integer,String> stringSendResult){
        log.info("Message Send Successfully for the key :{} and value is {}, partition is {}", key, value , stringSendResult.getRecordMetadata().partition());
    }

    public void handleFailure(int key, String value, Throwable ex){
        log.error("Error sending the Message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable e) {
            log.error("Error in OnFailure: {}", e.getMessage());
        }
    }

}
