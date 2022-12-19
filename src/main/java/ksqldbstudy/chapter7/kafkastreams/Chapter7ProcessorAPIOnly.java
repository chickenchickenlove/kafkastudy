package ksqldbstudy.chapter7.kafkastreams;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import ksqldbstudy.chapter7.domain.DigitalTwin;
import ksqldbstudy.chapter7.domain.TurbineState;
import ksqldbstudy.chapter7.kafkastreams.processor.DigitalTwinProcessor;
import ksqldbstudy.chapter7.kafkastreams.processor.HighWindsFlatmapProcessor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.UUID;

public class Chapter7ProcessorAPIOnly {
    public static void main(String[] args) {


        Topology topology = new Topology();


        GsonSerializer<DigitalTwin> digitalTwinGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<DigitalTwin> digitalTwinGsonDeserializer = new GsonDeserializer<>(DigitalTwin.class);

        GsonSerializer<TurbineState> turbineStateGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<TurbineState> turbineStateGsonDeserializer = new GsonDeserializer<>(TurbineState.class);

        Serde<DigitalTwin> digitalTwinSerde = Serdes.serdeFrom(digitalTwinGsonSerializer, digitalTwinGsonDeserializer);
        Serde<TurbineState> turbineStateSerde = Serdes.serdeFrom(turbineStateGsonSerializer, turbineStateGsonDeserializer);

        topology.addSource("reported-state-event-node", Serdes.String().deserializer(), turbineStateGsonDeserializer, "reported-state-events");
        topology.addSource("desired-state-event-node", Serdes.String().deserializer(), turbineStateGsonDeserializer, "desired-state-events");

        topology.addProcessor("high-winds-node", HighWindsFlatmapProcessor::new, "reported-state-event-node");

        StoreBuilder<KeyValueStore<String, DigitalTwin>> digitalTwinStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("digital-twin-store"),
                Serdes.String(), digitalTwinSerde);

        topology.addStateStore(digitalTwinStoreBuilder, "digital-twin-store-node");
        topology.addProcessor("digital-twin-store-node", () -> new DigitalTwinProcessor("digital-twin-store"));

        topology.addSink("digital-twin-sink",
                "digital-twins",
                Serdes.String().serializer(),
                digitalTwinGsonSerializer, "digital-twin-store-node");

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
