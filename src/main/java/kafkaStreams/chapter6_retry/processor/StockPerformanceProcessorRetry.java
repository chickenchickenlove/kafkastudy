package kafkaStreams.chapter6_retry.processor;

import kafkaStreams.chapter6_retry.puncuator.StockPerformancePunctuatorRetry;
import kafkaStreams.domain.StockPerformance;
import kafkaStreams.domain.StockTransaction;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

import static org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME;

public class StockPerformanceProcessorRetry extends ContextualProcessor<String, StockTransaction, String, StockPerformance> {

    private KeyValueStore<String, StockPerformance> keyValueStore;
    private final String stateStoreName;
    private final double differentialThreshold;


    public StockPerformanceProcessorRetry(String stateStoreName, double differentialThreshold) {
        this.differentialThreshold = differentialThreshold;
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext<String, StockPerformance> context) {
        super.init(context);

        // Processor가 초기화 될 때, 셋팅한다.
        keyValueStore = context.getStateStore(stateStoreName);
        StockPerformancePunctuatorRetry punctuatorRetry = new StockPerformancePunctuatorRetry(keyValueStore, context, differentialThreshold);
        context.schedule(Duration.ofSeconds(15), WALL_CLOCK_TIME, punctuatorRetry);
    }

    @Override
    public void process(Record<String, StockTransaction> record) {

        StockTransaction stockTransaction = record.value();
        String newKey = stockTransaction.getSymbol();

        if (newKey == null) {
            return;
        }

        StockPerformance stockPerformance = keyValueStore.get(newKey);

        if (stockPerformance == null) {
            stockPerformance = new StockPerformance();
        }

        stockPerformance.updatePriceStats(stockTransaction.getSharePrice());
        stockPerformance.updateVolumeStats(stockTransaction.getShares());
        stockPerformance.setLastUpdateSent(Instant.now());

        keyValueStore.put(newKey, stockPerformance);
    }
}
