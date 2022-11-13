package kafkaStreamsTest;

import kafkaStreams.domain.StockTickerData;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaStreams2 {

    public static void main(String[] args) {

        Serde<String> stringSerde = Serdes.String();

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> test_topic = streamsBuilder.stream(List.of("test-topic"));


        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("hello-state-store");
        StoreBuilder<KeyValueStore<String, String>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, stringSerde);
        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);


        KStream<String, String> stringStringKStream = test_topic.mapValues((readOnlyKey, value) -> value + " +++ topology-1");
        stringStringKStream.print(Printed.<String, String>toSysOut().withLabel("topology-1"));

        KStream<String, String> stringStringKStream2 = test_topic.mapValues((readOnlyKey, value) -> value + " ---  topology-2");
        stringStringKStream2.print(Printed.<String, String>toSysOut().withLabel("topology-2"));

        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(30)).after(Duration.ofMinutes(1));
        StringValueJoiner stringValueJoiner = new StringValueJoiner();
        KStream<String, String> joinedStream = stringStringKStream2.join(stringStringKStream, stringValueJoiner, joinWindows);

        joinedStream.print(Printed.<String, String>toSysOut().withLabel("JOINED"));



        Topology build = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(build, properties);
        kafkaStreams.start();
    }



}
