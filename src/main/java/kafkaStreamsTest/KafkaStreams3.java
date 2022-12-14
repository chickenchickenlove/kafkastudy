package kafkaStreamsTest;

import kafkaStreams.chapter5.StreamsSerdes;
import kafkaStreams.domain.StockTransaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaStreams3 {

    public static void main(String[] args) {

        Serde<String> stringSerde = Serdes.String();

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "application-10000000001");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        properties.setProperty(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "2");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        properties.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "10");

//        CooperativeStickyAssignor

        KeyValueBytesStoreSupplier hello = Stores.inMemoryKeyValueStore("hello");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, StockTransaction> test_topic = streamsBuilder.stream(List.of("stock-transactions"), Consumed.with(stringSerde, StreamsSerdes.StockTransactionSerde()));
        test_topic.print(Printed.toSysOut());

        KTable<String, Long> count = test_topic.selectKey((key, value) -> value.getSymbol())
                .groupByKey().count(Materialized.as(hello));


        Topology build = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(build, properties);
        kafkaStreams.start();









    }



}
