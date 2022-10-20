package kafkaStreams_test.chapter4;

import kafkaStreams.chapter3.GsonDeserializer;
import kafkaStreams.chapter3.GsonSerializer;
import kafkaStreams.chapter3.JsonSerializer;
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

import java.util.Properties;
import java.util.UUID;

@Slf4j
public class ZMartKafkaStreamsChapter4 {

    public static void main(String[] args) {

        String sourceTopic = "transactions";

        // Serde 설정
        GsonSerializer<Purchase> purchaseGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<Purchase> purchaseDeserializer = new GsonDeserializer<Purchase>();

        GsonSerializer<PurchasePattern> purchasePatternGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<PurchasePattern> purchasePatternGsonDeserializer = new GsonDeserializer<>();

        GsonSerializer<RewardAccumulator> rewardAccumulatorGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<RewardAccumulator> rewardAccumulatorGsonDeserializer = new GsonDeserializer<>();

        // Serde 생성
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        Serde<Integer> integerSerde = Serdes.Integer();

        Serde<PurchasePattern> purchasePatternSerde = Serdes.serdeFrom(purchasePatternGsonSerializer, purchasePatternGsonDeserializer);
        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorGsonSerializer, rewardAccumulatorGsonDeserializer);
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseGsonSerializer, purchaseDeserializer);

        // 설정
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, purchaseSerde.getClass().getName());

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

        // foreach 설정
//        ForeachAction<String, Purchase> purchaseDBForeach = (key, value) -> log.info("key = {}, value = {}", key, value);
//        purchaseKStream.foreach(purchaseDBForeach);


        // rewardAccumulatorKStream 처리
        String rewardsStateStoreName = "rewardsPointsStore";
        RewardStreamRepartition rewardStreamRepartition = new RewardStreamRepartition();

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, integerSerde);
        streamsBuilder.addStateStore(storeBuilder);


        // TODO : 확인
        KeyValueMapper<String, Purchase, String> repartitionKeyValueMapper = (key, value) -> value.getCustomerId();
        KStream<String, Purchase> stringPurchaseKStream = purchaseKStream.selectKey(repartitionKeyValueMapper);
        stringPurchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel("repartition"));
//
        Repartitioned<String, Purchase> repartitioned = Repartitioned.as("hello-this").with(stringSerde, purchaseSerde);
        KStream<String, Purchase> repartition = stringPurchaseKStream.repartition(repartitioned);
        repartition.print(Printed.<String, Purchase>toSysOut().withLabel("TEST"));



        // 아래는 동작하는 코드다.
        KStream<String, RewardAccumulator> stringRewardAccumulatorKStream = purchaseKStream.transformValues(() -> new RewardValueTransformer(rewardsStateStoreName), rewardsStateStoreName);
        stringRewardAccumulatorKStream.to("final", Produced.with(stringSerde, rewardAccumulatorSerde));
        stringRewardAccumulatorKStream.print(Printed.toSysOut());

        // sink 설정
        purchasePatternKStream.to("patterns",Produced.with(stringSerde,purchasePatternSerde));
        rewardAccumulatorKStream.to("rewards",Produced.with(stringSerde,rewardAccumulatorSerde));
        purchaseFilterStream.to("purchase",Produced.with(longSerde,purchaseSerde));

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

