package kafkaStreams.chapter5;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import kafkaStreams.domain.StockTickerData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.UUID;

public class SimpleStockStreams {

    private final static String STOCK_TICKER_TABLE_TOPIC = "stock-ticker-table";
    private final static String STOCK_TICKER_STREAM_TOPIC = "stock-ticker-stream";


    public static void main(String[] args) throws InterruptedException {



        GsonSerializer<StockTickerData> stockTickerDataGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTickerData> stockTickerDataGsonDeserializer = new GsonDeserializer<>(StockTickerData.class);

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTickerData> stockTickerDataSerde = Serdes.serdeFrom(stockTickerDataGsonSerializer, stockTickerDataGsonDeserializer);

        Properties properties = getProperties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stockTickerDataSerde.getClass().getName());

        StreamsConfig streamsConfig = new StreamsConfig(properties);

        StreamsBuilder builder = new StreamsBuilder();


        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("hello");
        StoreBuilder<KeyValueStore<String, StockTickerData>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, stockTickerDataSerde);

        // StateStore를 사용한 KTable → 이렇게 하면 최신값만 나옴

        builder.addStateStore(keyValueStoreStoreBuilder);
        KeyValueBytesStoreSupplier persistentKeyValueStore = Stores.persistentKeyValueStore("persistentKeyValueStore");
        Materialized<String, StockTickerData, KeyValueStore<Bytes, byte[]>> as = Materialized.as(persistentKeyValueStore);
        KTable<String, StockTickerData> stockTickerTable = builder.
                table(STOCK_TICKER_TABLE_TOPIC, Consumed.with(stringSerde, stockTickerDataSerde),
                        as);


        stockTickerTable.toStream().print(Printed.<String, StockTickerData>toSysOut().withLabel("Stocks-KTable-With-StateStore"));

//         StateStore를 사용하지 않은 KTable → 메세지가 전달될 때 마다 나옴
//        KTable<String, StockTickerData> stockTickerTable1 = builder.
//                table(STOCK_TICKER_TABLE_TOPIC, Consumed.with(stringSerde, stockTickerDataSerde));
//        stockTickerTable1.toStream().print(Printed.<String, StockTickerData>toSysOut().withLabel("Stocks-KTable-Without-StateStore"));

        // KStream → 메세지가 전달될 때마다 나옴.
        KStream<String, StockTickerData> stockTickerStream = builder.stream(STOCK_TICKER_STREAM_TOPIC,
                Consumed.with(stringSerde, stockTickerDataSerde));


        stockTickerStream.print(Printed.<String, StockTickerData>toSysOut().withLabel( "Stocks-KStream"));

//        MockDataProducer.produceStockTickerData(3, 3);



        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
//        LOG.info("KTable vs KStream output started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(15000);
//        LOG.info("Shutting down KTable vs KStream Application now");
//        kafkaStreams.close();
//        MockDataProducer.shutdown();
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KStreamVSKTable_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KStreamVSKTable_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1400000000");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 200);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return props;

    }


}
