package kafkaStreams.playground;

import kafkaStreams.chapter5.StreamsSerdes;
import kafkaStreams.domain.StockTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.scala.Serdes;

import java.util.Properties;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class playground1 {

    public static void main(String[] args) {


        Serde<String> stringSerde = Serdes.String();

        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "application-1");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,stringSerde.getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,stringSerde.getClass().getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("my-topic",
                Consumed.with(stringSerde, stringSerde).withOffsetResetPolicy(EARLIEST));

        KStream<String, String> repartition1 = stream.repartition();
        KStream<String, String> repartition2 = stream.repartition(Repartitioned.with(stringSerde, stringSerde).withNumberOfPartitions(20));

        repartition1.to("repartition-topic1",Produced.with(stringSerde, stringSerde));
        repartition2.to("repartition-topic2",Produced.with(stringSerde, stringSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();
    }




}

