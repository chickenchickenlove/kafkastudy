package kafkaStreams.chapter6.processor;

import kafkaStreams.domain.ClickEvent;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.Tuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

import static org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME;

public class MyStockTransactionProcessorWithPunctuator extends ContextualProcessor<String, StockTransaction, String, StockTransaction> {

    private KeyValueStore<String, StockTransaction> keyValueStore;
    private ProcessorContext context;
    private String stateStoreName;

    public MyStockTransactionProcessorWithPunctuator(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }


    @Override
    public void init(ProcessorContext<String, StockTransaction> context) {
        super.init(context);
        this.context = context;
        keyValueStore = context.getStateStore(stateStoreName);
        context.schedule(Duration.ofSeconds(15), WALL_CLOCK_TIME, this::myPunctuate);
    }

    @Override
    public void process(Record<String, StockTransaction> record) {
        String key = record.key();
        String nextKey = "[NEXT-KEY]" + key;
        keyValueStore.put(nextKey, record.value());
    }

    private void myPunctuate(long timeStamp) {
        KeyValueIterator<String, StockTransaction> all = keyValueStore.all();
        while (all.hasNext()) {
            KeyValue<String, StockTransaction> next = all.next();
            Record<String, StockTransaction> stringStockTransactionRecord = new Record<String, StockTransaction>(next.key, next.value, timeStamp);
            context.forward(stringStockTransactionRecord);
        }
    }
}
