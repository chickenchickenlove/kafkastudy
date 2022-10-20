package kafkaStreams.chapter4_test;

import kafkaStreams.domain.Purchase;
import kafkaStreams.domain.RewardAccumulator;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class RewardValueTransform implements ValueTransformer<Purchase, RewardAccumulator> {

    private ProcessorContext context;
    private KeyValueStore<String, Integer> store;
    private final String storeName;

    public RewardValueTransform(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        store = (KeyValueStore) this.context.getStateStore(storeName);
    }

    @Override
    public RewardAccumulator transform(Purchase value) {

        RewardAccumulator rewardAccumulator = RewardAccumulator.builder(value).build();
        Integer stateValue = this.store.get(rewardAccumulator.getCustomerId());

        if (stateValue != null) {
            rewardAccumulator.addRewardPoints(stateValue);
        }

        this.store.put(rewardAccumulator.getCustomerId(), rewardAccumulator.getTotalRewardPoints());
        return rewardAccumulator;
    }

    @Override
    public void close() {

    }
}
