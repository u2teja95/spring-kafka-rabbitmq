package com.spring.connect.kafka.rabbitmq.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.connect.kafka.rabbitmq.model.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
@Service
public class Consumer {
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    private AmqpAdmin amqpAdmin;

    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @KafkaListener(topics = "test", containerFactory = "kafkaListenerContainerFactory")
    public void consume(Output output, Acknowledgment acknowledgment) {
        producer(output);
        acknowledgment.acknowledge();
    }

    private void producer(Output out) {
        try {
            String[] keys = out.getCid().split(":");

            Queue queue = new Queue(keys[0], true, false, true);
            Binding binding = new Binding(keys[0], Binding.DestinationType.QUEUE, "test", keys[1], null);
            amqpAdmin.declareQueue(queue);
            amqpAdmin.declareBinding(binding);

            System.out.println("Send msg to consumer= " + out);

            rabbitTemplate.convertAndSend("test", keys[1], new ObjectMapper().writeValueAsString(out));
        } catch (Exception e){

        }



    }
}
