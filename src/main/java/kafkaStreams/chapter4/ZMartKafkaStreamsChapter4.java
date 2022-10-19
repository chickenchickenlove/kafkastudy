package kafkaStreams.chapter4;

import kafkaStreams.chapter3.GsonDeserializer;
import kafkaStreams.chapter3.GsonSerializer;
import kafkaStreams.chapter3.JsonDeserializer;
import kafkaStreams.chapter3.JsonSerializer;
import kafkaStreams.domain.Purchase;
import kafkaStreams.domain.PurchasePattern;
import kafkaStreams.domain.RewardAccumulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.internals.DefaultStreamPartitioner;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class ZMartKafkaStreamsChapter4 {

    private static final String STORE_NAME = "rewardAccumulator";

    public static void main(String[] args) {

        String sourceTopic = "transactions";

        // Serde 설정

        GsonSerializer<Purchase> purchaseGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<Purchase> purchaseGsonDeserializer = new GsonDeserializer<>(Purchase.class);

        GsonSerializer<PurchasePattern> purchasePatternGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<PurchasePattern> purchasePatternGsonDeserializer = new GsonDeserializer<>(PurchasePattern.class);

        GsonSerializer<RewardAccumulator> rewardAccumulatorGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<RewardAccumulator> rewardAccumulatorGsonDeserializer = new GsonDeserializer<>(RewardAccumulator.class);

        // Serde 생성
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        Serde<PurchasePattern> purchasePatternSerde = Serdes.serdeFrom(purchasePatternGsonSerializer, purchasePatternGsonDeserializer);
        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorGsonSerializer, rewardAccumulatorGsonDeserializer);
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseGsonSerializer, purchaseGsonDeserializer);
        Serde<Integer> integerSerde = Serdes.Integer();


        // 설정
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());

        // topology 생성 시작
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Purchase> purchaseKStream = streamsBuilder.stream(sourceTopic, Consumed.with(stringSerde, purchaseSerde))
                .mapValues(value -> Purchase.builder(value).maskCreditCard().build());


        // 리파티셔닝용 키 설정
        KeyValueMapper<String, Purchase, Long> purchaseFilterStreamNewKey = (key, value) -> value.getPurchaseDate().getTime();


        // Processor 생성
        KStream<String, PurchasePattern> purchasePatternKStream = purchaseKStream.mapValues(value -> PurchasePattern.builder(value).build());
        KStream<String, RewardAccumulator> rewardAccumulatorKStream = purchaseKStream.mapValues(value -> RewardAccumulator.builder(value).build());
        KStream<Long, Purchase> purchaseFilterStream = purchaseKStream.filter((key, value) -> value.getPrice() > 5.00).selectKey(purchaseFilterStreamNewKey);


        // StateStore 처리하기
        KeyValueMapper<String, Purchase, String> customerIdKeyValueMapper = (key, purchase) -> purchase.getCustomerId();
        Repartitioned<String, Purchase> repartitioned = Repartitioned.with(stringSerde, purchaseSerde).withNumberOfPartitions(3);
        KStream<String, Purchase> repartitionStream = purchaseKStream.selectKey(customerIdKeyValueMapper).repartition(repartitioned);


        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(STORE_NAME);
        StoreBuilder<KeyValueStore<String, Integer>> keyValueStoreStoreBuilder = Stores.<String, Integer>keyValueStoreBuilder(storeSupplier, stringSerde, integerSerde);
        streamsBuilder.addStateStore(keyValueStoreStoreBuilder);

        repartitionStream.transformValues(() ->
                new RewardValueTransformer(STORE_NAME), STORE_NAME).to("STATE_REPARTITION",
                Produced.with(stringSerde, rewardAccumulatorSerde));

        repartitionStream.print(Printed.<String, Purchase>toSysOut().withLabel("HELLO"));


        // foreach 설정
        ForeachAction<String, Purchase> purchaseDBForeach = (key, value) -> log.info("key = {}, value = {}", key, value);
        purchaseKStream.foreach(purchaseDBForeach);

        // print 설정
//        purchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel("purchaseKStream"));
//        purchasePatternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("purchasePattern"));
//        rewardAccumulatorKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewardAccumulatorKStream"));
//        purchaseFilterStream.print(Printed.<Long, Purchase>toSysOut().withLabel("purchaseFilterStream"));

        // sink 설정
        purchasePatternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));
        rewardAccumulatorKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));
        purchaseFilterStream.to("purchase", Produced.with(longSerde, purchaseSerde));

        // stream 시작
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

    }

    private static <T> JsonSerializer<T> createJsonSerializer() {
        return new JsonSerializer<T>();
    }

    private static <T> GsonDeserializer<T> createGsonDeserializer(Class type) {
        return new GsonDeserializer<>(type);
    }


}

