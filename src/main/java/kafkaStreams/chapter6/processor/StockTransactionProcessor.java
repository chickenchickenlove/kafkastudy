package kafkaStreams.chapter6.processor;

import kafkaStreams.domain.ClickEvent;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.Tuple;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class StockTransactionProcessor extends ContextualProcessor<String, StockTransaction, String, Tuple<ClickEvent, StockTransaction>> {

    @Override
    public void process(Record<String, StockTransaction> record) {
        String key = record.key();
        StockTransaction value = record.value();
        if (key == null) {
            return;
        }
        Tuple<ClickEvent, StockTransaction> tuple = Tuple.of(null, value);
        Record<String, Tuple<ClickEvent, StockTransaction>> generatedRecord = new Record<String, Tuple<ClickEvent, StockTransaction>>(key, tuple, record.timestamp());
        context().forward(generatedRecord);
    }
}
