package kafkaStreams.chapter6.processor;

import kafkaStreams.chapter6.puncuator.CogroupPunctuator;
import kafkaStreams.domain.ClickEvent;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.processor.PunctuationType.STREAM_TIME;

public class CogroupingProcessorWithPunctuator extends ContextualProcessor<String, Tuple<ClickEvent, StockTransaction>, String, Tuple<ClickEvent, StockTransaction>> {

    private KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValueStore;
    public String storeName;

    public CogroupingProcessorWithPunctuator(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, Tuple<ClickEvent, StockTransaction>> context) {
        super.init(context);
        keyValueStore = context.getStateStore(storeName);
        context.schedule(Duration.ofSeconds(15), STREAM_TIME, this::cogroup);
    }

    @Override
    public void process(Record<String, Tuple<ClickEvent, StockTransaction>> record) {
        String key = record.key();
        Tuple<ClickEvent, StockTransaction> value = record.value();
        Tuple<List<ClickEvent>, List<StockTransaction>> cogroupedTuple = keyValueStore.get(key);
        if (cogroupedTuple == null) {
            cogroupedTuple = Tuple.of(new ArrayList<>(), new ArrayList<>());
        }

        if (value._1 != null) {
            cogroupedTuple._1.add(value._1);
        }

        if (value._2 != null) {
            cogroupedTuple._2.add(value._2);
        }

        keyValueStore.put(key, cogroupedTuple);
    }

    public void cogroup(long timestamp) {
        KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator = keyValueStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValue = iterator.next();
            Tuple<List<ClickEvent>, List<StockTransaction>> value = keyValue.value;

            if (value != null && (!value._1.isEmpty()) && (!value._2.isEmpty())) {
                Record record = new Record(keyValue.key, keyValue.value, timestamp);
                context().forward(record);
            }
        }


    }
}