package ksqldbstudy.chapter5.producer;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ksqldbstudy.chapter5.domain.Pulse;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Chapter5Producer {


    private static List<ProducerRecord<String, String>> createPulseRecord() {

        ArrayList<ProducerRecord<String, String>> ret = new ArrayList<>();
        Faker faker = new Faker();

        Gson gson = new Gson();

        Gson gson1 = new GsonBuilder().create();

        for (int i = 0; i < 1000; i++) {
            String customNumber = String.valueOf(faker.number().numberBetween(0, 2));
            Pulse pulse = new Pulse(faker.date().past(1, TimeUnit.MINUTES).toInstant().toString());
            String jsonPulse = gson1.toJson(pulse);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("pulse-events",customNumber, jsonPulse);
            ret.add(record);
        }
        return ret;
    }

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        while (true) {
            List<ProducerRecord<String, String>> pulseRecord = createPulseRecord();
            for (ProducerRecord<String, String> stringStringProducerRecord : pulseRecord) {
                kafkaProducer.send(stringStringProducerRecord);
                Thread.sleep(30);
            }
        }


    }




}
