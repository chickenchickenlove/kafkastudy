package kafkaStreams.chapter8;

import kafkaStreams.domain.Purchase;
import kafkaStreams.domain.PurchasePattern;
import kafkaStreams.domain.RewardAccumulator;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class ZMartTopology {

    public static Topology build() {


        GsonSerializer<Purchase> purchaseGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<Purchase> purchaseGsonDeserializer = new GsonDeserializer<>(Purchase.class);

        GsonSerializer<PurchasePattern> purchasePatternGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<PurchasePattern> purchasePatternGsonDeserializer1 = new GsonDeserializer<>(PurchasePattern.class);

        GsonSerializer<RewardAccumulator> rewardAccumulatorGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<RewardAccumulator> rewardAccumulatorGsonDeserializer = new GsonDeserializer<>(RewardAccumulator.class);

        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseGsonSerializer, purchaseGsonDeserializer);
        Serde<PurchasePattern> purchasePatternSerde = Serdes.serdeFrom(purchasePatternGsonSerializer, purchasePatternGsonDeserializer1);
        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorGsonSerializer, rewardAccumulatorGsonDeserializer);
        Serde<String> stringSerde = Serdes.String();


        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Purchase> purchaseKStream = streamsBuilder.stream(
                        "transactions",
                        Consumed.with(stringSerde, purchaseSerde).withOffsetResetPolicy(EARLIEST))
                .mapValues((readOnlyKey, value) -> Purchase.builder(value).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());
        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("[Purchase-Pattern]"));

        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.mapValues(value -> RewardAccumulator.builder(value).build());

        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));
        rewardsKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));
        purchaseKStream.to("purchases", Produced.with(Serdes.String(), purchaseSerde));

        return streamsBuilder.build();
    }

}
