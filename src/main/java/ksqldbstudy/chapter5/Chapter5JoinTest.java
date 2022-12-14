package ksqldbstudy.chapter5;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import ksqldbstudy.chapter5.domain.BodyTemp;
import ksqldbstudy.chapter5.domain.Pulse;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

public class Chapter5JoinTest {

    @Getter
    public static class CustomTestJoiner{

        private String topicA;
        private String topicB;

        public CustomTestJoiner(String topicA, String topicB) {
            this.topicA = topicA;
            this.topicB = topicB;
        }
    }


    public static void main(String[] args) {



        GsonSerializer<CustomTestJoiner> customTestJoinerGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<CustomTestJoiner> customTestJoinerGsonDeserializer = new GsonDeserializer<>(CustomTestJoiner.class);
        Serde<CustomTestJoiner> customTestJoinerSerde = Serdes.serdeFrom(customTestJoinerGsonSerializer, customTestJoinerGsonDeserializer);


        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());



        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> topicA = streamsBuilder.stream("topicA", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> topicB = streamsBuilder.stream("topicB", Consumed.with(Serdes.String(), Serdes.String()));


        KeyValueMapper<String, String, String> stringStringStringKeyValueMapper = new KeyValueMapper<>() {
            @Override
            public String apply(String key, String value) {
                return "hello";
            }
        };

//        KStream<String, String> map = topicA.map(stringStringStringKeyValueMapper, Named.as("hello"));
//        KStream<Object, Object> hello = topicA.map((key, value) -> value, Named.as("hello"));



        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60));
        SlidingWindows joinWindows1 = SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60));

        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));
        ValueJoiner<String, String, CustomTestJoiner> valueJoiner = CustomTestJoiner::new;
        KStream<String, CustomTestJoiner> joinedStream = topicA.join(topicB, valueJoiner, joinWindows,
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));


        KGroupedStream<String, CustomTestJoiner> stream1 = joinedStream.groupByKey();
        TimeWindowedKStream<String, CustomTestJoiner> stream2 = stream1.windowedBy(timeWindows);
        KTable<Windowed<String>, Long> count = stream2.count();


        joinedStream.print(Printed.<String, CustomTestJoiner>toSysOut().withLabel("[HELLO-LABEL]"));
//
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();


    }

}
