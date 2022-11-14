package kafkaStreams.chapter6.processor;

import kafkaStreams.domain.ClickEvent;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.Tuple;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class ClickEventProcessor extends ContextualProcessor<String, ClickEvent, String, Tuple<ClickEvent, StockTransaction>> {
    @Override
    public void process(Record<String, ClickEvent> record) {
        String key = record.key();
        ClickEvent clickEvent = record.value();

        if (key == null) {
            return;
        }

        Tuple<ClickEvent, StockTransaction> tuple = Tuple.of(clickEvent, null);
        Record<String, Tuple<ClickEvent, StockTransaction>> generatedRecord = new Record<String, Tuple<ClickEvent, StockTransaction>>(key, tuple, record.timestamp());
        context().forward(generatedRecord);
    }
}
