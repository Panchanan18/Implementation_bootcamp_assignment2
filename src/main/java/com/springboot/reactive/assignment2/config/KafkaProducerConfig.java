package com.springboot.reactive.assignment2.config;
import com.springboot.reactive.assignment2.constants.KafkaConstants;
import com.springboot.reactive.assignment2.entity.EmployeeRequest;
import com.springboot.reactive.assignment2.entity.EmployeeRequestSerializer;
import com.springboot.reactive.assignment2.entity.EmployeeSerDes;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {





    @Bean
    public ProducerFactory<String, EmployeeRequest> producerFactory(){
        Map<String,Object> configProps= new HashMap<String,Object>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.HOST);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,     StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EmployeeSerDes.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean(name= "kafkaTemplate")
    public KafkaTemplate<String,EmployeeRequest>  kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

}
