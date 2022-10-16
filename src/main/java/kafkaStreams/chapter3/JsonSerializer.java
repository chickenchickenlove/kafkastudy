package kafkaStreams.chapter3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {


    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        byte[] bytes;

        try {
            bytes = mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return bytes;
    }
}
