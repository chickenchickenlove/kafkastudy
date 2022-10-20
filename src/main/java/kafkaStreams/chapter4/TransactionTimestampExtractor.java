package kafkaStreams.chapter4;

import kafkaStreams.domain.Purchase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TransactionTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Purchase value = (Purchase) record.value();
        return value.getPurchaseDate().getTime();
    }
}
