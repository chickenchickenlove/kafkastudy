package kafkaStreams.chapter7;

import kafkaStreams.domain.StockTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@Slf4j
public class StockTransactionProducerInterceptor implements ProducerInterceptor<String, StockTransaction> {

    @Override
    public ProducerRecord<String, StockTransaction> onSend(ProducerRecord<String, StockTransaction> record) {
        log.info("Producer Record On Send");
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        log.info("Producer Record On Acknowledgement");
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
