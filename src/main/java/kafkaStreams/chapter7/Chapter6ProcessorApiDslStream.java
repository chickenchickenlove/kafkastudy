package kafkaStreams.chapter7;

import com.google.gson.internal.Streams;
import kafkaStreams.chapter5.InitProducerProperty;
import kafkaStreams.chapter6.processor.MyStockTransactionProcessorWithPunctuator;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class Chapter6ProcessorApiDslStream {

    public static void main(String[] args) {


        Properties initProperty = new Properties();
        Properties props = InitProducerProperty.initProperty(initProperty);

        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
                List.of(StockTransactionConsumerInterceptor.class.getName()));
        props.setProperty(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG), StockTransactionConsumerInterceptor.class.getName());
        props.put(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG),
                List.of(StockTransactionProducerInterceptor.class.getName(),
                        ClicksProducerInterceptor.class.getName()));


        GsonSerializer<StockTransaction> stockTransactionGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTransaction> stockTransactionGsonDeserializer = new GsonDeserializer<>(StockTransaction.class);

        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionGsonSerializer, stockTransactionGsonDeserializer);
        Serde<String> stringSerde = Serdes.String();


        StreamsBuilder builder = new StreamsBuilder();
        String storeName = "hello-store";

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(storeName);
        StoreBuilder<KeyValueStore<String, StockTransaction>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, stockTransactionSerde);

        builder.addStateStore(keyValueStoreStoreBuilder);

        builder
                .stream("stock-transactions",
                        Consumed.with(stringSerde, stockTransactionSerde).withOffsetResetPolicy(EARLIEST))
                .process(() -> new MyStockTransactionProcessorWithPunctuator(storeName), storeName)
                .print(Printed.toSysOut());

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
    }

}
