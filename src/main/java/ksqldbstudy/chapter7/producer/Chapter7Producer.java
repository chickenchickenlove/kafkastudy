package ksqldbstudy.chapter7.producer;

import com.github.javafaker.DateAndTime;
import com.github.javafaker.Faker;
import com.github.javafaker.Number;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ksqldbstudy.chapter7.domain.Power;
import ksqldbstudy.chapter7.domain.TurbineState;
import ksqldbstudy.chapter7.domain.Type;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Chapter7Producer {

    public static List<String> createTurbineState(Power power, Type type) {

        ArrayList<String> turbineStates = new ArrayList<>();
        Faker faker = new Faker();
        Number fakerNumber = faker.number();
        DateAndTime fakerDate = faker.date();

        Gson gson = new GsonBuilder().create();

        for (int i = 0; i < 100; i++) {
            Date past = fakerDate.past(2, TimeUnit.HOURS);
            String pastDate = past.toInstant().toString();
            double speed = fakerNumber.randomDouble(1, 1, 100);
            TurbineState turbineState = new TurbineState(pastDate,
                    speed, power, type);
            String jsonParsed = gson.toJson(turbineState);
            turbineStates.add(jsonParsed);
        }

        return turbineStates;
    }

    public static void main(String[] args) throws InterruptedException {


        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        while (true) {
            List<String> reportedState = createTurbineState(Power.ON, Type.REPORTED);
            List<String> desiredState = createTurbineState(Power.OFF, Type.DESIRED);

            for (String value : desiredState) {
                ProducerRecord<String, String> record = new ProducerRecord<>("desired-state-events","1", value);
                kafkaProducer.send(record);
            }

            for (String value : reportedState) {
                ProducerRecord<String, String> record = new ProducerRecord<>("reported-state-events","1", value);
                kafkaProducer.send(record);
            }

            Thread.sleep(1000);
        }



    }


}
