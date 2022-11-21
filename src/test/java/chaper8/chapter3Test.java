package chaper8;


import kafkaStreams.chapter8.ZMartTopology;
import kafkaStreams.domain.Purchase;
import kafkaStreams.domain.PurchasePattern;
import kafkaStreams.domain.RewardAccumulator;
import kafkaStreams.util.DataGenerator;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.test.TestRecord;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

public class chapter3Test {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Purchase> inputTopic;
    private TestOutputTopic<String, Purchase> outputTopicPurchase;
    private TestOutputTopic<String, RewardAccumulator> outputTopicRewardAccumulator;
    private TestOutputTopic<String, PurchasePattern> outputTopicPurchasePattern;

    @Before
    public void setUp() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-kafka-Streams-App");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");

        StreamsConfig streamsConfig = new StreamsConfig(props);
        Topology topology = ZMartTopology.build();
        topologyTestDriver = new TopologyTestDriver(topology, props);


        GsonSerializer<Purchase> purchaseGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<Purchase> purchaseGsonDeserializer = new GsonDeserializer<>(Purchase.class);

        GsonSerializer<PurchasePattern> purchasePatternGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<PurchasePattern> purchasePatternGsonDeserializer = new GsonDeserializer<>(PurchasePattern.class);

        GsonSerializer<RewardAccumulator> recordAccumulatorGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<RewardAccumulator> recordAccumulatorGsonDeserializer = new GsonDeserializer<>(RewardAccumulator.class);

        Serde<RewardAccumulator> rewardAccumulator = Serdes.serdeFrom(recordAccumulatorGsonSerializer, recordAccumulatorGsonDeserializer);
        Serde<PurchasePattern> purchasePatternSerde = Serdes.serdeFrom(purchasePatternGsonSerializer, purchasePatternGsonDeserializer);
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseGsonSerializer, purchaseGsonDeserializer);
        Serde<String> stringSerde = Serdes.String();

        // input
        inputTopic = topologyTestDriver.createInputTopic("transactions", stringSerde.serializer(), purchaseSerde.serializer());

        // output
        outputTopicPurchase = topologyTestDriver.createOutputTopic("purchases",stringSerde.deserializer(), purchaseSerde.deserializer());
        outputTopicRewardAccumulator = topologyTestDriver.createOutputTopic("rewards",stringSerde.deserializer(), rewardAccumulator.deserializer());
        outputTopicPurchasePattern = topologyTestDriver.createOutputTopic("patterns",stringSerde.deserializer(), purchasePatternSerde.deserializer());
    }



    @Test
    @DisplayName("outputTopic Purchase Test")
    public void this1() {
        Purchase purchase = DataGenerator.generatePurchase();
        Purchase purchase1 = DataGenerator.generatePurchase();
        inputTopic.pipeInput(purchase);
        inputTopic.pipeInput(purchase1);
        Purchase outputValue = outputTopicPurchase.readValue();
        Purchase expectedValue = Purchase.builder(purchase).maskCreditCard().build();
        assertThat(outputValue).isEqualTo(expectedValue);
    }

    @Test
    @DisplayName("outputTopic Record Accumulator Test")
    public void this2() {
        Purchase purchase = DataGenerator.generatePurchase();
        Purchase purchase1 = DataGenerator.generatePurchase();
        inputTopic.pipeInput(purchase);
        RewardAccumulator outputValue = outputTopicRewardAccumulator.readValue();
        RewardAccumulator expectedValue = RewardAccumulator.builder(purchase).build();
        assertThat(outputValue).isEqualTo(expectedValue);
    }

    @Test
    @DisplayName("Testing the Purchasepattern")
    public void this3() {
        Purchase purchase = DataGenerator.generatePurchase();
        inputTopic.pipeInput(purchase);
        PurchasePattern outputValue = outputTopicPurchasePattern.readValue();
        PurchasePattern expectedValue = PurchasePattern.builder(purchase).build();
        assertThat(outputValue).isEqualTo(expectedValue);
    }


}
