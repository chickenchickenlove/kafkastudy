package ksqldbstudy.chapter5;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import ksqldbstudy.chapter5.domain.BodyTemp;
import ksqldbstudy.chapter5.domain.CombineVital;
import ksqldbstudy.chapter5.domain.Pulse;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

public class Chapter5PulseStreams {

    public static void main(String[] args) {


        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        GsonSerializer<Pulse> pulseGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<Pulse> pulseGsonDeserializer = new GsonDeserializer<>(Pulse.class);
        GsonSerializer<BodyTemp> bodyTempGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<BodyTemp> bodyTempGsonDeserializer = new GsonDeserializer<>(BodyTemp.class);
        GsonSerializer<CombineVital> combineVitalGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<CombineVital> combineVitalGsonDeserializer = new GsonDeserializer<>(CombineVital.class);

        Serde<Pulse> pulseSerde = Serdes.serdeFrom(pulseGsonSerializer, pulseGsonDeserializer);
        Serde<BodyTemp> bodyTempSerde = Serdes.serdeFrom(bodyTempGsonSerializer, bodyTempGsonDeserializer);
        Serde<CombineVital> combineVitalSerde = Serdes.serdeFrom(combineVitalGsonSerializer, combineVitalGsonDeserializer);


        StreamsBuilder builder = new StreamsBuilder();

        // Source
        KStream<String, Pulse> pulseKStream = builder.stream("pulse-counts", Consumed.with(Serdes.String(), pulseSerde).withTimestampExtractor(new FailOnInvalidTimestamp()));
        KStream<String, BodyTemp> bodyTempKStream = builder.stream("body-temp-", Consumed.with(Serdes.String(), bodyTempSerde));

        // heart beat BPM 정의
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(5));
        KTable<Windowed<String>, Long> pulseCount = pulseKStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count(Materialized.as("pulse-counts"));

        KTable<Windowed<String>, Long> suppressedPulseCount = pulseCount.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));
        KStream<String, Long> bpmStream = suppressedPulseCount
                .toStream()
                .filter((key, value) -> value > 100)
                .map((key, value) -> KeyValue.pair(key.key(), value));

        // temp filter
        KStream<String, BodyTemp> highTempStream = bodyTempKStream
                .filter((key, value) -> value.getTemperature() > 37.5);

        // Join 처리
        ValueJoiner<Long, BodyTemp, CombineVital> valueJoiner = CombineVital::new;
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60));
        KStream<String, CombineVital> joinedStream = bpmStream.join(highTempStream, valueJoiner, joinWindows);

        // out
        joinedStream.to("final",
                Produced.with(Serdes.String(), combineVitalSerde));

        // Kafka Streams 시작
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

}
