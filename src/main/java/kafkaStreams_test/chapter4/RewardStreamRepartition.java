package kafkaStreams_test.chapter4;

import kafkaStreams.domain.Purchase;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RewardStreamRepartition implements StreamPartitioner<String, Purchase> {
    @Override
    public Integer partition(String topic, String key, Purchase value, int numPartitions) {
        System.out.println("Repartition");
        return value.getCustomerId().hashCode() % numPartitions;
    }
}
