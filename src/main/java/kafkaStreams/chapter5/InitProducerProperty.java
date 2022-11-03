package kafkaStreams.chapter5;

import com.google.gson.internal.Streams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.UUID;

public class InitProducerProperty {

    public static Properties initProperty(Properties props) {
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        return props;
    }






}
