package kafkaStreams.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafkaStreams.chapter3.FixedSizePriorityQueue;
import kafkaStreams.chapter3.FixedSizePriorityQueueAdapter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.processor.internals.GlobalStreamThread;

import java.lang.reflect.Type;
import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {



    private Gson gson;
    private Class<T> deserializedClass;
    private Type reflectionTypeToken;

    public GsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
        init();
    }

    public GsonDeserializer(Type reflectionTypeToken) {
        this.reflectionTypeToken = reflectionTypeToken;
        init();
    }

    private void init () {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(FixedSizePriorityQueue.class, new FixedSizePriorityQueueAdapter().nullSafe());
        builder.registerTypeAdapter(GsonUtils.LocalDateAdapter.class, new GsonUtils.LocalDateAdapter())
                .registerTypeAdapter(GsonUtils.LocalTimeAdapter.class, new GsonUtils.LocalTimeAdapter())
                .registerTypeAdapter(GsonUtils.LocalDateTimeAdapter.class, new GsonUtils.LocalDateTimeAdapter());
        gson = builder.create();
    }

    public GsonDeserializer() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> map, boolean b) {
        if(deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        Type deserializeFrom = deserializedClass != null ? deserializedClass : reflectionTypeToken;

        return gson.fromJson(new String(bytes),deserializeFrom);

    }

    @Override
    public void close() {

    }
}
