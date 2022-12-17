package ksqldbstudy.chapter11.producer;

import com.github.javafaker.DateAndTime;
import com.github.javafaker.Faker;
import com.github.javafaker.Number;
import com.google.gson.Gson;
import ksqldbstudy.chapter11.domain.NetflixSession;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class chapter11Producer {

    private static List<String> createNetflixSessionList(boolean wantPast) {

        ArrayList<String> ret = new ArrayList<>();

        Faker faker = new Faker();
        Number number = faker.number();
        DateAndTime fakerDate = faker.date();
        Gson gson = new Gson().newBuilder().create();


        for (int i = 0; i < 100; i++) {
            int sessionId = number.numberBetween(0, 50);
            int titleId = number.numberBetween(0, 50);
            Date date;

            if (wantPast) {
                date = fakerDate.past(10, TimeUnit.MINUTES);
            }else {
                date = fakerDate.future(10, TimeUnit.MINUTES);
            }

            NetflixSession netflixSession = new NetflixSession(
                    sessionId, titleId, date);
            String json = gson.toJson(netflixSession);
            ret.add(json);
        }
        return ret;
    }

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaProducer.close()));

        while (true) {

            List<String> beforeSessionList = createNetflixSessionList(true);
            List<String> afterSessionList = createNetflixSessionList(false);

            for (String beforeSession : beforeSessionList) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("start-session", null, beforeSession);
                kafkaProducer.send(producerRecord);
            }

            for (String afterSession : afterSessionList) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("end-session", null, afterSession);
                kafkaProducer.send(producerRecord);
            }

            Thread.sleep(10000);
        }
    }






}
