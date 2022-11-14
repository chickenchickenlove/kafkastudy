package kafkaStreams.chapter6;

import kafka.server.KafkaConfig;
import kafkaStreams.chapter5.InitProducerProperty;
import kafkaStreams.chapter6.processor.ClickEventProcessor;
import kafkaStreams.chapter6.processor.CogroupingProcessor;
import kafkaStreams.chapter6.processor.CogroupingProcessorWithPunctuator;
import kafkaStreams.chapter6.processor.StockTransactionProcessor;
import kafkaStreams.domain.ClickEvent;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import kafkaStreams.util.Tuple;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.internals.KStreamPrint;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class Chapter6CoGroupStream {

    public static void main(String[] args) {


        Properties initProperty = new Properties();
        Properties props = InitProducerProperty.initProperty(initProperty);


        GsonSerializer<StockTransaction> stockTransactionGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTransaction> stockTransactionGsonDeserializer = new GsonDeserializer<>(StockTransaction.class);

        GsonSerializer<ClickEvent> clickEventGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<ClickEvent> clickEventGsonDeserializer = new GsonDeserializer<>(ClickEvent.class);

        GsonSerializer<Tuple<List<ClickEvent>, List<StockTransaction>>> tupleGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<Tuple<List<ClickEvent>, List<StockTransaction>>> tupleGsonDeserializer = new GsonDeserializer<Tuple<List<ClickEvent>, List<StockTransaction>>>(Tuple.class);

        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionGsonSerializer, stockTransactionGsonDeserializer);
        Serde<ClickEvent> clickEventSerde = Serdes.serdeFrom(clickEventGsonSerializer, clickEventGsonDeserializer);
        Serde<String> stringSerde = Serdes.String();
        Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> tupleSerde = Serdes.serdeFrom(tupleGsonSerializer, tupleGsonDeserializer);

        HashMap<String, String> changeLogProps = new HashMap<>();
        changeLogProps.put("retention.ms", "120000");
        changeLogProps.put("cleanup.policy", "compact, delete");

        Topology topology = new Topology();

        String storeName = "tupleCoGroupStore";
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);
        StoreBuilder<KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>>> keyValueStoreStoreBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, stringSerde, tupleSerde).withLoggingEnabled(changeLogProps);

        topology.addSource(EARLIEST
                        , "Txn-source",
                        stringSerde.deserializer(),
                        stockTransactionSerde.deserializer(),
                        "stock-transactions")
                .addSource(EARLIEST,
                        "Events-source",
                        stringSerde.deserializer(),
                        clickEventSerde.deserializer(),
                        "events")
                .addProcessor("Txn-processor",
                        StockTransactionProcessor::new,
                        "Txn-source")
                .addProcessor("Events-Processor",
                        ClickEventProcessor::new,
                        "Events-source")
                .addProcessor("CoGrouping-Processor",
                        () -> new CogroupingProcessorWithPunctuator(storeName),
                        "Txn-processor", "Events-Processor")
                .addStateStore(keyValueStoreStoreBuilder, "CoGrouping-Processor")
                .addProcessor("printer",
                        new KStreamPrint<>((key, value) -> System.out.println("[RESULT] ::: key = " + key + " value = " + value)),
                        "CoGrouping-Processor")
                .addSink("Tuple-sink",
                        "cogrouped-results", stringSerde.serializer(), tupleSerde.serializer(),
                        "CoGrouping-Processor");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
    }


}
