package ksqldbstudy.chapter7.kafkastreams;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import ksqldbstudy.chapter7.domain.DigitalTwin;
import ksqldbstudy.chapter7.domain.Power;
import ksqldbstudy.chapter7.domain.TurbineState;
import ksqldbstudy.chapter7.domain.Type;
import ksqldbstudy.chapter7.kafkastreams.processor.DigitalTwinProcessor;
import ksqldbstudy.chapter7.kafkastreams.processor.HighWindsFlatmapProcessor;
import ksqldbstudy.chapter7.kafkastreams.transformer.DigitalTwinTransformer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.*;

public class Chapter7ProcessorHybrid {
    public static void main(String[] args) {

        GsonSerializer<DigitalTwin> digitalTwinGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<DigitalTwin> digitalTwinGsonDeserializer = new GsonDeserializer<>(DigitalTwin.class);

        GsonSerializer<TurbineState> turbineStateGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<TurbineState> turbineStateGsonDeserializer = new GsonDeserializer<>(TurbineState.class);

        Serde<DigitalTwin> digitalTwinSerde = Serdes.serdeFrom(digitalTwinGsonSerializer, digitalTwinGsonDeserializer);
        Serde<TurbineState> turbineStateSerde = Serdes.serdeFrom(turbineStateGsonSerializer, turbineStateGsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, TurbineState> reportedStream = builder.stream("reported-state-events", Consumed.with(Serdes.String(), turbineStateSerde));
        KStream<String, TurbineState> desiredStream = builder.stream("desired-state-events", Consumed.with(Serdes.String(), turbineStateSerde));

        // filter + merge
        KStream<String, TurbineState> mergedProcessor = reportedStream.flatMapValues((ValueMapperWithKey<String, TurbineState, Iterable<TurbineState>>) (readOnlyKey, reportedValue) -> {
                    List<TurbineState> turbineStates = new ArrayList<>();
                    turbineStates.add(reportedValue);
                    if (reportedValue.getWindSpeedMph() > 65 && reportedValue.getPower().equals(Power.ON)) {
                        TurbineState desired = reportedValue.clone();
                        desired.setType(Type.DESIRED);
                        desired.setPower(Power.OFF);
                        turbineStates.add(desired);
                    }
                    return turbineStates;
                }
        ).merge(desiredStream);

        StoreBuilder<KeyValueStore<String, DigitalTwin>> digitalTwinStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("digital-twin-store"),
                Serdes.String(), digitalTwinSerde);

        builder.addStateStore(digitalTwinStoreBuilder);

        KStream<String, DigitalTwin> lastProcessor = mergedProcessor.process(() -> new DigitalTwinProcessor("digital-twin-store"), "digital-twin-store");
        lastProcessor.to("digital-twins", Produced.with(Serdes.String(), digitalTwinSerde));
        lastProcessor.print(Printed.<String, DigitalTwin>toSysOut().withLabel("[HELLO]"));




        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
