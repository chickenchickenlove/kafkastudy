package ksqldbstudy.chapter10.producer;


import com.github.javafaker.*;
import com.github.javafaker.Number;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ksqldbstudy.chapter10.domain.ProductionChange;
import ksqldbstudy.chapter10.domain.SessionLength;
import ksqldbstudy.chapter10.domain.Title;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class NetflixProducer {

    private static List<String> changeTypes = List.of("season_length", "season_begin", "season_end");



    private static List<String> createTitleList() {
        Faker faker = new Faker();
        Number fakerNumber = faker.number();
        Commerce commerce = faker.commerce();

        Gson gson = new GsonBuilder().create();
        ArrayList<String> ret = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            long id = fakerNumber.numberBetween(0L, 50L);
            String titleName = commerce.productName();
            boolean trueFalse = faker.bool().bool();
            Title title = new Title(id, titleName, trueFalse);

            String json = gson.toJson(title);
            ret.add(json);
        }
        return ret;
    }

    private static List<String> createdProductionChanges() {
        Faker faker = new Faker();
        Number fakerNumber = faker.number();
        DateAndTime date = faker.date();
        ArrayList<String> ret = new ArrayList<>();

        Gson gson = new GsonBuilder().create();

        for (int i = 0; i < 100; i++) {
            long beforeSeasonId = fakerNumber.numberBetween(0L, 100L);
            long afterSeasonId = fakerNumber.numberBetween(0L, 100L);
            long beforeCount = fakerNumber.numberBetween(0L, 1000L);
            long afterCount = fakerNumber.numberBetween(0L, 1000L);
            SessionLength before = new SessionLength(beforeSeasonId, beforeCount);
            SessionLength after = new SessionLength(afterSeasonId, afterCount);
            int index = fakerNumber.numberBetween(0, changeTypes.size() - 1);
            long titleId = fakerNumber.numberBetween(0, 50L);
            ProductionChange productionChange = new ProductionChange(
                    UUID.randomUUID().toString(), titleId, changeTypes.get(index),
                    before, after, date.past(3, TimeUnit.DAYS));
            String json = gson.toJson(productionChange);
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
            List<String> titleList = createTitleList();
            List<String> productionChangesList = createdProductionChanges();

            List<ProducerRecord<String, String>> titleRecord = titleList.
                    stream().map(value -> new ProducerRecord<String, String>("titles",null, value))
                    .collect(Collectors.toList());

            List<ProducerRecord<String, String>> productionChangesRecords = productionChangesList.
                    stream().map(value -> new ProducerRecord<String, String>("production_changes",null, value))
                    .collect(Collectors.toList());


            for (ProducerRecord<String, String> record : titleRecord) {
                kafkaProducer.send(record);
            }

            for (ProducerRecord<String, String> record : productionChangesRecords) {
                kafkaProducer.send(record);
            }

            Thread.sleep(6000);
        }

    }

}
