package kafkaStreams.chapter3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private Class<T> type;

    public JsonDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
            return objectMapper.readValue(data, type);
        } catch (IOException e) {
            System.out.println("HERE");
            System.out.println(data.toString());


            throw new RuntimeException(e);
        }
    }
}
