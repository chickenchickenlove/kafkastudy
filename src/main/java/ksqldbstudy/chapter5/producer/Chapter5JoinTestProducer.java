package ksqldbstudy.chapter5.producer;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ksqldbstudy.chapter5.domain.Pulse;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Chapter5JoinTestProducer {



    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        while (true) {
            ProducerRecord<String, String> topicA = new ProducerRecord<String, String>("topicA", "a", UUID.randomUUID().toString());
            ProducerRecord<String, String> topicB = new ProducerRecord<String, String>("topicA", "a", UUID.randomUUID().toString());
            kafkaProducer.send(topicA);
            kafkaProducer.send(topicB);
            Thread.sleep(30);
        }


    }




}
