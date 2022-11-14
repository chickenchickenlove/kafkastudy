package kafkaStreams.chapter7;

import kafkaStreams.domain.StockTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@Slf4j
public class StockTransactionConsumerInterceptor implements ConsumerInterceptor<String, StockTransaction> {
    @Override
    public ConsumerRecords<String, StockTransaction> onConsume(ConsumerRecords<String, StockTransaction> records) {
        log.info("[ASH-TEST] ConsumerRecord on-Consume");
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.info("[ASH-TEST] ConsumerRecord Commit");
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
