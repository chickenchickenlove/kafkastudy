package kafkaStreams.chapter6_retry;

import kafkaStreams.chapter6.processor.StockPerformanceProcessor;
import kafkaStreams.chapter6_retry.processor.StockPerformanceProcessorRetry;
import kafkaStreams.domain.StockPerformance;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.internals.KStreamPrint;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class Chapter6StockStreamRetry {

    public static void main(String[] args) {


        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());


        GsonSerializer<StockTransaction> stockTransactionGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTransaction> stockTransactionGsonDeserializer = new GsonDeserializer<>(StockTransaction.class);

        GsonSerializer<StockPerformance> stockPerformanceGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockPerformance> stockPerformanceGsonDeserializer = new GsonDeserializer<>(StockPerformance.class);


        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionGsonSerializer, stockTransactionGsonDeserializer);
        Serde<StockPerformance> stockPerformanceSerde = Serdes.serdeFrom(stockPerformanceGsonSerializer, stockPerformanceGsonDeserializer);

        Topology topology = new Topology();
        String stocksStateStore = "stock-performance-store";
        double differentialThreshold = 0.02;

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(stocksStateStore);
        StoreBuilder<KeyValueStore<String, StockPerformance>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, stockPerformanceSerde);

        topology
                .addSource(EARLIEST,
                        "stocks-source",
                        stringSerde.deserializer(),
                        stockTransactionSerde.deserializer(),
                        "stock-transactions")
                .addProcessor("stocks-processor",
                        () -> new StockPerformanceProcessorRetry(stocksStateStore, differentialThreshold),
                        "stocks-source")
                .addStateStore(keyValueStoreStoreBuilder,
                        "stocks-processor")
                .addProcessor("printer",
                        new KStreamPrint<>((key, value) -> System.out.println("key = " + key + " value = " + value)), "stocks-processor")
                .addSink("stocks-sink",
                        "stock-performance",
                        stringSerde.serializer(), stockPerformanceSerde.serializer(),
                        "stocks-processor");


        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
    }

}
