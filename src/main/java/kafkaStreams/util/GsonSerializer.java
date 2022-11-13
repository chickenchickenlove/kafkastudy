package kafkaStreams.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import kafkaStreams.chapter3.FixedSizePriorityQueue;
import kafkaStreams.chapter3.FixedSizePriorityQueueAdapter;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.Charset;
import java.util.Map;

public class GsonSerializer<T> implements Serializer<T> {

    private Gson gson;

    public GsonSerializer() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(FixedSizePriorityQueue.class, new FixedSizePriorityQueueAdapter().nullSafe());
        builder.registerTypeAdapter(GsonUtils.LocalDateAdapter.class, new GsonUtils.LocalDateAdapter())
                .registerTypeAdapter(GsonUtils.LocalTimeAdapter.class, new GsonUtils.LocalTimeAdapter())
                .registerTypeAdapter(GsonUtils.LocalDateTimeAdapter.class, new GsonUtils.LocalDateTimeAdapter());
        gson = builder.create();
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, T t) {
        return gson.toJson(t).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public void close() {

    }
}
