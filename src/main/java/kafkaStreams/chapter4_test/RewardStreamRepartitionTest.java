package kafkaStreams.chapter4_test;

import kafkaStreams.domain.Purchase;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Map;

public class RewardStreamRepartitionTest implements StreamPartitioner<String, Purchase> {

    @Override
    public Integer partition(String topic, String key, Purchase value, int numPartitions) {
        return value.getCustomerId().hashCode() % 3;
    }
}
