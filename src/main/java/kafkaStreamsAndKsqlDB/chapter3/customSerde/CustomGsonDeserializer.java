package kafkaStreamsAndKsqlDB.chapter3.customSerde;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafkaStreams.domain.Tweet;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class CustomGsonDeserializer<T> implements Deserializer<T> {

    private Gson gson;
    private Class<T> deserializedClass;


    public CustomGsonDeserializer(Class<T> deserializedClass) {
        this.gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .create();
        this.deserializedClass = deserializedClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), deserializedClass);
    }
}
