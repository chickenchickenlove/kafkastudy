package kafkaStreams.chapter4_test;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import kafkaStreams.chapter3.JsonSerializer;
import kafkaStreams.chapter4.RewardValueTransformer;
import kafkaStreams.domain.CorrelatedPurchase;
import kafkaStreams.domain.Purchase;
import kafkaStreams.domain.PurchasePattern;
import kafkaStreams.domain.RewardAccumulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class ZMartKafkaStreamsChapter4Again {

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

        GsonSerializer<CorrelatedPurchase> correlatedPurchaseGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<CorrelatedPurchase> correlatedPurchaseGsonDeserializer = new GsonDeserializer<>(CorrelatedPurchase.class);

        // Serde 생성
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        Serde<PurchasePattern> purchasePatternSerde = Serdes.serdeFrom(purchasePatternGsonSerializer, purchasePatternGsonDeserializer);
        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorGsonSerializer, rewardAccumulatorGsonDeserializer);
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseGsonSerializer, purchaseGsonDeserializer);
        Serde<Integer> integerSerde = Serdes.Integer();
        Serde<CorrelatedPurchase> correlatedPurchaseSerde = Serdes.serdeFrom(correlatedPurchaseGsonSerializer, correlatedPurchaseGsonDeserializer);


        // 설정
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractorRetry.class.getName());

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




//         브랜치를 새로 따고 Join까지 해보자.
        Map<String, KStream<String, Purchase>> branchStream = purchaseKStream.selectKey((key, purchase) -> purchase.getCustomerId())
                .split(Named.as("MyBranch-"))
                .branch((key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee"), Branched.as("coffee"))
                .branch((key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics"), Branched.as("electronics"))
                .noDefaultBranch();


        KStream<String, Purchase> coffeeStream = branchStream.get("MyBranch-coffee");
        KStream<String, Purchase> electronicStream = branchStream.get("MyBranch-electronics");

        PurchaseJoinerRetry purchaseJoinerRetry = new PurchaseJoinerRetry();
        JoinWindows windows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(20)).after(Duration.ofMillis(5));

        KStream<String, CorrelatedPurchase> joinedStream = coffeeStream.join(electronicStream,
                purchaseJoinerRetry, windows, StreamJoined.with(stringSerde, purchaseSerde, purchaseSerde));

        // foreach 설정
        ForeachAction<String, Purchase> purchaseDBForeach = (key, value) -> log.info("key = {}, value = {}", key, value);
        purchaseKStream.foreach(purchaseDBForeach);

        // print 설정
//        purchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel("purchaseKStream"));
//        purchasePatternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("purchasePattern"));
//        rewardAccumulatorKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewardAccumulatorKStream"));
//        purchaseFilterStream.print(Printed.<Long, Purchase>toSysOut().withLabel("purchaseFilterStream"));
//        repartitionStream.print(Printed.<String, Purchase>toSysOut().withLabel("HELLO"));
        joinedStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("JOINED-STREAM"));


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

