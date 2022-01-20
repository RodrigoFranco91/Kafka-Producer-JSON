package br.com.rodrigo.producer.producer;

import br.com.rodrigo.producer.dto.CarDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class CarProducer {

    private static final Logger logger = LoggerFactory.getLogger(CarProducer.class);
    private String topic;
    private KafkaTemplate<String, CarDTO> kafkaTemplate;

    //@Value está pegando lá do application.properties
    public CarProducer(@Value("${topic.name}") String topic, KafkaTemplate<String, CarDTO> kafkaTemplate){
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(CarDTO carDTO){
        kafkaTemplate.send(topic, carDTO).addCallback(
                succesS -> logger.info("A mensagem foi enviada "+ succesS.getProducerRecord().value()),
                failure -> logger.info("Mensagem falhou " + failure.getMessage())
        );
    }
}
