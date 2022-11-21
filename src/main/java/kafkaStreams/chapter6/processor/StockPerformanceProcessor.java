package kafkaStreams.chapter6.processor;

import kafkaStreams.chapter6.puncuator.StockPerformancePunctuator;
import kafkaStreams.domain.StockPerformance;
import kafkaStreams.domain.StockTransaction;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

public class StockPerformanceProcessor extends ContextualProcessor<String, StockTransaction, String, StockPerformance> {

    private KeyValueStore<String, StockPerformance> keyValueStore;
    private String stateStoreName;
    private double differentialThreshold;

    public StockPerformanceProcessor(String stateStoreName, double differentialThreshold) {
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        keyValueStore = context.getStateStore(stateStoreName);
        StockPerformancePunctuator punctuator = new StockPerformancePunctuator(differentialThreshold, context, keyValueStore);
        super.context().schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, punctuator);
    }


    @Override
    public void process(Record<String, StockTransaction> record) {

        StockTransaction stockTransaction = record.value();
        String symbol = stockTransaction.getSymbol();

        if (symbol == null) {
            return;
        }
        StockPerformance stockPerformance = keyValueStore.get(symbol);

        if (stockPerformance == null) {
            stockPerformance = new StockPerformance();
        }

        stockPerformance.updatePriceStats(stockTransaction.getSharePrice());
        stockPerformance.updateVolumeStats(stockTransaction.getShares());
        stockPerformance.setLastUpdateSent(Instant.now());

        keyValueStore.put(symbol, stockPerformance);
    }
}
