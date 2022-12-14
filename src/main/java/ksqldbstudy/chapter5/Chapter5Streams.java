package ksqldbstudy.chapter5;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import ksqldbstudy.chapter5.domain.BodyTemp;
import ksqldbstudy.chapter5.domain.Pulse;
import ksqldbstudy.chapter5.timestamp.PulseTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.*;
import org.apache.kafka.streams.kstream.internals.suppress.KTableSuppressProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.suppress.StrictBufferConfigImpl;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Chapter5Streams {

    public static void main(String[] args) {


        GsonSerializer<BodyTemp> bodyTempGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<BodyTemp> bodyTempGsonDeserializer = new GsonDeserializer<>(BodyTemp.class);
        GsonSerializer<Pulse> pulseGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<Pulse> pulseGsonDeserializer = new GsonDeserializer<>(Pulse.class);

        Serde<BodyTemp> bodyTempSerde = Serdes.serdeFrom(bodyTempGsonSerializer, bodyTempGsonDeserializer);
        Serde<Pulse> pulseSerde = Serdes.serdeFrom(pulseGsonSerializer, pulseGsonDeserializer);

        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, FailOnInvalidTimestamp.class.getName());


        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Pulse> pulseStream = streamsBuilder.stream("pulse-events",
                Consumed.with(Serdes.String(), pulseSerde, new FailOnInvalidTimestamp(), Topology.AutoOffsetReset.EARLIEST));

//        KStream<String, Pulse> pulseStream1 = streamsBuilder.stream("pu",
//                Consumed.with(Serdes.String(), pulseSerde, new FailOnInvalidTimestamp(), Topology.AutoOffsetReset.EARLIEST));

//        KStreamSlidingWindowAggregate
//        KStreamSessionWindowAggregate
//        KStreamWindowAggregate
//        KStreamAggregate.

        KGroupedStream<String, Pulse> groupedStream1 = pulseStream.groupByKey();
        KTable<String, Long> count = groupedStream1.count();

        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60));
        KTable<Windowed<String>, Long> count1 = groupedStream1.windowedBy(timeWindows).count();

        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10));

        // Stream끼리 조인할 때 Window를 사용한다.

        KGroupedStream<String, Pulse> stringPulseKGroupedStream = pulseStream.groupByKey();
        // Grouped Stream이 된다는 것은 무엇을 의미할까?

        TimeWindows tumbling = TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10));
        TimeWindowedKStream<String, Pulse> timeWindowedKStream = stringPulseKGroupedStream.windowedBy(tumbling);

        // Key가 Windowed로 바뀜. Windowed는 Key / window를 가지고 있음.
        KTable<Windowed<String>, Long> countWindow = timeWindowedKStream.count(Materialized.as(
                Stores.persistentWindowStore("pulse-count", Duration.ofSeconds(60), Duration.ofSeconds(60), true)));

        // 우선 나오도록 만들게 하기
        countWindow.toStream().print(Printed.<Windowed<String>, Long>toSysOut().withLabel("[HELLO]"));


        KTable<Windowed<String>, Long> suppress = countWindow.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
//        suppress.toStream().print(Printed.<Windowed<String>, Long>toSysOut().withLabel("[SUPRRESED]"));


        KStream<String, Long> highPulse = suppress.toStream()
                .filter((key, value) -> value > 100)
                .map((key, value) -> KeyValue.pair(key.key(), value));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();


    }



}
