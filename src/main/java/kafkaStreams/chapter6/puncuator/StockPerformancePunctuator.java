package kafkaStreams.chapter6.puncuator;

import kafkaStreams.domain.StockPerformance;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;



// 이곳에서 주요한 로직을 처리한다.
// 10분에 한번씩 DonwStream StateStore를 전부 찾아보고 데이터를 내려준다.
public class StockPerformancePunctuator implements Punctuator {

    private double differentialThreshold;
    private ProcessorContext context;
    private KeyValueStore<String, StockPerformance> keyValueStore;

    public StockPerformancePunctuator(double differentialThreshold, ProcessorContext context, KeyValueStore<String, StockPerformance> keyValueStore) {
        this.differentialThreshold = differentialThreshold;
        this.context = context;
        this.keyValueStore = keyValueStore;
    }


    @Override
    public void punctuate(long timestamp) {

        System.out.println("timestamp = " + timestamp);
        KeyValueIterator<String, StockPerformance> keyValueIterator = keyValueStore.all();

        while (keyValueIterator.hasNext()) {

            KeyValue<String, StockPerformance> keyValue = keyValueIterator.next();
            String key = keyValue.key;
            StockPerformance value = keyValue.value;

            System.out.println("Stored value = " + value);
            if (value != null) {
                if (value.priceDifferential() >= differentialThreshold || value.volumeDifferential() >= differentialThreshold) {
                    Record<String, StockPerformance> stringStockPerformanceRecord = new Record<String, StockPerformance>(key, value, timestamp);
                    System.out.println("forwarded");
                    context.forward(stringStockPerformanceRecord);
                }
            }
        }
    }
}
