package kafkaStreams.chapter6.processor;

import kafkaStreams.domain.StockTransaction;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class MyStockTransactionProcessor extends ContextualProcessor<String, StockTransaction, String, StockTransaction> {


    @Override
    public void process(Record<String, StockTransaction> record) {
        String key = record.key();
        String nextKey = "[NEXT-KEY]" + key;
        context().forward(new Record<String, StockTransaction>(nextKey, record.value(), record.timestamp()));
    }
}
