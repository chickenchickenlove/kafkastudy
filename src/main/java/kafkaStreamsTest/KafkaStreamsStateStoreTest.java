package kafkaStreamsTest;

import kafkaStreams.chapter5.StreamsSerdes;
import kafkaStreams.domain.StockTransaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class      KafkaStreamsStateStoreTest {

    public static void main(String[] args) {

        Serde<String> stringSerde = Serdes.String();

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "100");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "application-test");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, "C:\\kafka-streams");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, StockTransaction> test_topic = streamsBuilder.stream(List.of("stock-transactions"), Consumed.with(stringSerde, StreamsSerdes.StockTransactionSerde()));

        Properties properties1 = new Properties();
        properties1.setProperty("log.message.timestamp.type", TimestampType.CREATE_TIME.toString());
        properties1.setProperty("log.message.timestamp.type", TimestampType.LOG_APPEND_TIME.toString());


//        KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore("hello-store");
        KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.persistentKeyValueStore("hello-store");
        StoreBuilder<KeyValueStore<String, Long>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(keyValueBytesStoreSupplier, stringSerde, Serdes.Long());

        KTable<String, Long> count = test_topic.selectKey((key, value) -> value.getSymbol())
                .groupByKey(Grouped.with(Serdes.String(), StreamsSerdes.StockTransactionSerde()))
                .count(Materialized.as(keyValueBytesStoreSupplier));


        test_topic.print(Printed.toSysOut());

        Topology build = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(build, properties);
        kafkaStreams.start();

        Properties props = new Properties();
        props.setProperty(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "10");
    }



}
