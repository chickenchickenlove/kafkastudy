package ksql.CountryCSVContents;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CountryCsvProvider {

    private static List<String> key = List.of("AU", "IN", "GB", "US");
    private static List<String> valueList = List.of("Au, Australia", "In, India", "GB, UK", "Us, United States");


    public static void main(String[] args) throws InterruptedException, ExecutionException {

        String topicName = "COUNTRY-CSV";
        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


        for (int i = 0; i < key.size(); i++) {
            System.out.println("i = " + i);
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>(topicName, key.get(i), valueList.get(i));
            RecordMetadata recordMetadata = producer.send(producerRecord).get();
            System.out.println("recordMetadata = " + recordMetadata.offset());
        }

        Thread.sleep(10000);
    }

}
