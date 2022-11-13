package kafkaStreamsTest;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class StringValueJoiner implements ValueJoiner<String, String, String> {
    @Override
    public String apply(String value1, String value2) {
        String ret = "joined " + value1 + " ||||||||||||||| " + value2;
        return ret;
    }
}
