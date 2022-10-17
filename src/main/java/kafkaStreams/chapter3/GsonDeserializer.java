package kafkaStreams.chapter3;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

public class GsonDeserializer<T> implements Deserializer<T> {


    private Gson gson = new Gson();
    private Class<T> deserializedClass;


    public GsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    public GsonDeserializer() {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return gson.fromJson(new String(data), deserializedClass);
    }
}
