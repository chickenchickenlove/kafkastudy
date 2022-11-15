package kafkaStreams.chapter7.chapter7interceptor;

import kafkaStreams.domain.ClickEvent;
import kafkaStreams.domain.StockTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;


@Slf4j
public class ClicksProducerInterceptor implements ProducerInterceptor<String, ClickEvent> {


    @Override
    public ProducerRecord<String, ClickEvent> onSend(ProducerRecord<String, ClickEvent> record) {
        log.info("Producer Record On Send");
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        log.info("Producer Record On Ack");
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
