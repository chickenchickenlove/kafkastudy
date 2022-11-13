package kafkaStreams.chapter6_retry.puncuator;

import kafkaStreams.domain.StockPerformance;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class StockPerformancePunctuatorRetry implements Punctuator {

    private KeyValueStore<String, StockPerformance> keyValueStore;
    private ProcessorContext context;
    private double differentialThreshold;

    public StockPerformancePunctuatorRetry(KeyValueStore<String, StockPerformance> keyValueStore, ProcessorContext context, double differentialThreshold) {
        this.keyValueStore = keyValueStore;
        this.context = context;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, StockPerformance> iterator = keyValueStore.all();
        while (iterator.hasNext()) {
            KeyValue<String, StockPerformance> keyValue = iterator.next();
            String key = keyValue.key;
            StockPerformance value = keyValue.value;

            if (value != null) {
                if (value.priceDifferential() >= differentialThreshold || value.volumeDifferential() >= differentialThreshold) {
                    Record<String, StockPerformance> record = new Record<String, StockPerformance>(key, value, timestamp);
                    context.forward(record);
                }
            }
        }


    }
}
