package kafkaStreams.myconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class MyBroker {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {


        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.102:9092,192:168.56.103:9092,192.168.56.104:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        for (int i = 1100 ; i < 1200; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("my-topic", String.valueOf(i), String.valueOf(i));
            Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
            RecordMetadata recordMetadata = send.get(1000, TimeUnit.SECONDS);
            log.info("topic = {}, partition = {}, key = {}", recordMetadata.topic(), recordMetadata.partition(), producerRecord.key());
        }
    }
}

