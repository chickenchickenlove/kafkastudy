package kafkaStreams.chapter7;

import kafkaStreams.chapter3.JsonSerializer;
import kafkaStreams.domain.Purchase;
import kafkaStreams.domain.PurchasePattern;
import kafkaStreams.domain.RewardAccumulator;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.streams.KafkaStreams.State.CREATED;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;

@Slf4j
public class ZMartKafkaStreamsApp {

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


        // 설정
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());

        // 메트릭 설정
        props.setProperty(StreamsConfig.CLIENT_ID_CONFIG,"metrics-client-id");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"metrics-group-id");
        props.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");



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


        // sink 설정
        purchasePatternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));
        rewardAccumulatorKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));
        purchaseFilterStream.to("purchase", Produced.with(longSerde, purchaseSerde));


        Topology topology = streamsBuilder.build();
        TopologyDescription describe = topology.describe();

        Set<TopologyDescription.Subtopology> subtopologies = describe.subtopologies();
        for (TopologyDescription.Subtopology subtopology : subtopologies) {
            System.out.println("subtopology = " + subtopology);
        }



        Set<TopologyDescription.GlobalStore> globalStores = describe.globalStores();
        for (TopologyDescription.GlobalStore globalStore : globalStores) {
            System.out.println("globalStore = " + globalStore);
        }
        KafkaStreams.StateListener stateListener = new KafkaStreams.StateListener() {
            @Override
            public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
                if (newState == CREATED) {
                    log.info("ASHASHASHASH KAFKA STREAMS ");
                }

                if (newState == RUNNING) {
                    log.info("ASHASHASHASH KAFKA STREAMS RUNNING");
                }

            }
        };


        StreamsUncaughtExceptionHandler streamsUncaughtExceptionHandler = exception -> {
            log.info("exception raise");
            return null;
        };


        StateRestoreListener stateRestoreListener = new StateRestoreListener() {
            // 각 토픽 파티션 별로 복구해야할 offset을 기록한다.
            private Map<TopicPartition, Long> totalToRestore = new ConcurrentHashMap<>();

            // 각 토픽 파티션 별로 최종 복구된 offset를 기록한다.
            private Map<TopicPartition, Long> restoredSoFar = new ConcurrentHashMap<>();

            @Override
            public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
                long toRestore = endingOffset - startingOffset;
                totalToRestore.put(topicPartition, toRestore);
                log.info("start restore. restore target : {}, topicPartition : {}, record to Restore : {}", storeName, topicPartition, toRestore);
            }

            @Override
            public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {

                // 복원된 전체 레코드 수 계산
                long currentProgress = batchEndOffset + restoredSoFar.getOrDefault(topicPartition, 0L);
                double percentComplete = (double) currentProgress / totalToRestore.get(topicPartition);
                log.info("restore progress : {}, progress Percent : {}, Target store : {}, topicPartition : {}",
                        currentProgress, percentComplete, storeName, topicPartition );

                // 복원된 레코드 수를 저장한다.
                restoredSoFar.put(topicPartition, currentProgress);
            }

            @Override
            public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
                log.info("Restore Completed : {}, topicPartition : {}", storeName, topicPartition);
            }
        };


        // stream 시작
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.setStateListener(stateListener);
        kafkaStreams.setUncaughtExceptionHandler(streamsUncaughtExceptionHandler);
        kafkaStreams.setGlobalStateRestoreListener(stateRestoreListener);


        kafkaStreams.start();


    }

}

