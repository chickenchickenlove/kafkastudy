package kafkaStreamsTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaStreams1 {

    public static void main(String[] args) {

        Serde<String> stringSerde = Serdes.String();

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> test_topic = streamsBuilder.stream(List.of("test-topic", "test-topic2", "test-topic3"));
        test_topic.print(Printed.toSysOut());

        Topology build = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(build, properties);
        kafkaStreams.start();
    }



}
