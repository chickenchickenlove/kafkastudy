package ksqldbstudy.chapter7.kafkastreams.processor;

import ksqldbstudy.chapter7.domain.DigitalTwin;
import ksqldbstudy.chapter7.domain.TurbineState;
import ksqldbstudy.chapter7.domain.Type;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

public class DigitalTwinProcessor implements Processor<String, TurbineState, String, DigitalTwin> {

    private ProcessorContext<String, DigitalTwin> context;
    private KeyValueStore<String, DigitalTwin> store;
    private String storeName;

    public DigitalTwinProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<String, DigitalTwin> context) {
        Processor.super.init(context);
        this.context = context;
        this.store = context.getStateStore(this.storeName);
        context.schedule(Duration.ofMinutes(5), PunctuationType.WALL_CLOCK_TIME, this::enforceTtl);
    }

    @Override
    public void process(Record<String, TurbineState> record) {

        String key = record.key();
        TurbineState value = record.value();
        DigitalTwin digitalTwin = this.store.get(key);
        if (digitalTwin == null) {
            digitalTwin = new DigitalTwin();
        }

        if (value.getType() == Type.DESIRED) {
            digitalTwin.setDesired(value);
        } else {
            digitalTwin.setReported(value);
        }

        this.store.put(key, digitalTwin);
        Record<String, DigitalTwin> newRecord = new Record<>(key, digitalTwin, record.timestamp());
        this.context.forward(newRecord);
    }

    @Override
    public void close() {

    }

    private void enforceTtl(long timestamp) {
        try (KeyValueIterator<String, DigitalTwin> keyValueiterator = this.store.all()) {

            while (keyValueiterator.hasNext()) {
                KeyValue<String, DigitalTwin> entry = keyValueiterator.next();
                TurbineState lastReportedTurbineState = entry.value.getReported();
                if (lastReportedTurbineState == null) {
                    continue;
                }
                Instant lastUpdate = Instant.parse(lastReportedTurbineState.getTimestamp());
                long daysSinceLastUpdate = Duration.between(lastUpdate, Instant.now()).toDays();
                if (daysSinceLastUpdate >= 7) {
                    this.store.delete(entry.key);
                }
            }
        }
    }
}
