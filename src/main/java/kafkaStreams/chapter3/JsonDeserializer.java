package kafkaStreams.chapter3;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper objectMapper;
    private Class<T> type;

    public JsonDeserializer(ObjectMapper objectMapper, Class<T> type) {
        this.objectMapper = objectMapper;
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            System.out.println("Type = " + type.getName());

            return objectMapper.readValue(data, type);
        } catch (IOException e) {
            System.out.println("HERE");
            System.out.println(data.toString());


            throw new RuntimeException(e);
        }
    }
}
