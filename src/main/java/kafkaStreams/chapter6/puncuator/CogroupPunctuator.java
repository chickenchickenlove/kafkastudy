package kafkaStreams.chapter6.puncuator;

import kafkaStreams.domain.ClickEvent;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class CogroupPunctuator implements Punctuator {

    private KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValueStore;
    private ProcessorContext context;

    public CogroupPunctuator(KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValueStore, ProcessorContext context) {
        this.keyValueStore = keyValueStore;
        this.context = context;
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator = keyValueStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValue = iterator.next();
            Tuple<List<ClickEvent>, List<StockTransaction>> value = keyValue.value;

            if (value != null && (!value._1.isEmpty()) && (!value._2.isEmpty())) {
                Record record = new Record(keyValue.key, keyValue.value, timestamp);
                context.forward(record);
            }
        }
    }
}
