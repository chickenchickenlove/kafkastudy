package kafkaStreams.chapter5;

import com.google.gson.internal.Streams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.UUID;

public class InitProducerProperty {

    public static Properties initProperty(Properties props) {
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "app-2");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "C:/kafkastreams/test/");
        return props;
    }






}
