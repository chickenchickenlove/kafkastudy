package kafkaStreams.chapter8;


import kafkaStreams.domain.StockTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

import static org.apache.kafka.streams.processor.PunctuationType.STREAM_TIME;

@Slf4j
public class MyProcessorWithPunctuator implements Processor<String, StockTransaction, String, StockTransaction> {

    private String storeName;
    private KeyValueStore<String, StockTransaction> keyValueStore;
    private ProcessorContext processorContext;

    public MyProcessorWithPunctuator(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, StockTransaction> context) {
        Processor.super.init(context);
        this.processorContext = context;
        keyValueStore = context.getStateStore(storeName);
        processorContext.schedule(Duration.ofSeconds(15), STREAM_TIME, this::myPunctuate);
    }

    @Override
    public void process(Record<String, StockTransaction> record) {
        String key = record.key();
        StockTransaction value = record.value();
        keyValueStore.put(key, value);
        processorContext.forward(new Record("new-" + key, value, record.timestamp()));
    }

    @Override
    public void close() {
        Processor.super.close();
    }

    private void myPunctuate(long timeStamp) {
        KeyValueIterator<String, StockTransaction> all = keyValueStore.all();
        while (all.hasNext()) {
            KeyValue<String, StockTransaction> next = all.next();
            processorContext.forward(new Record(next.key, next.value, SystemTime.SYSTEM.milliseconds()));
        }
    }
}



