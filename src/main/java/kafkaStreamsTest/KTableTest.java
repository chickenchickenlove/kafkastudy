package kafkaStreamsTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class KTableTest {
    public static void main(String[] args) {


        Serde<Integer> intSerde = Serdes.Integer();
        Serde<String> stringSerde = Serdes.String();


        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Integer> sourceStream = streamsBuilder.stream("test-ktable", Consumed.with(stringSerde, intSerde));
//        sourceStream.groupBy((key, value) -> key).aggregate(() -> )





    }
}
