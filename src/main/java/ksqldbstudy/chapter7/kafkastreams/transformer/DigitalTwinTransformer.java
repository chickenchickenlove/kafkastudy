package ksqldbstudy.chapter7.kafkastreams.transformer;

import ksqldbstudy.chapter7.domain.DigitalTwin;
import ksqldbstudy.chapter7.domain.TurbineState;
import ksqldbstudy.chapter7.domain.Type;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

public class DigitalTwinTransformer implements ValueTransformerWithKey<String, TurbineState, Iterable<DigitalTwin>> {

    private KeyValueStore<String, DigitalTwin> store;
    private String storeName;

    public DigitalTwinTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.store = context.getStateStore(storeName);
        context.schedule(Duration.ofMinutes(5), PunctuationType.WALL_CLOCK_TIME, this::enforceTtl);
    }

    @Override
    public Iterable<DigitalTwin> transform(String readOnlyKey, TurbineState value) {
        DigitalTwin digitalTwin = this.store.get(readOnlyKey);
        if (digitalTwin == null) {
            digitalTwin = new DigitalTwin();
        }

        if (value.getType() == Type.DESIRED) {
            digitalTwin.setDesired(value);
        } else {
            digitalTwin.setReported(value);
        }

        this.store.put(readOnlyKey, digitalTwin);
        return Collections.singletonList(digitalTwin);
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
