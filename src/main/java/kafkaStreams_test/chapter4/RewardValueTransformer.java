package kafkaStreams_test.chapter4;

import kafkaStreams.domain.Purchase;
import kafkaStreams.domain.RewardAccumulator;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class RewardValueTransformer implements ValueTransformer<Purchase, RewardAccumulator> {

    private ProcessorContext context;
    private String storeName;
    private KeyValueStore<String, Integer> stateStore;


    public RewardValueTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore)context.getStateStore(storeName);
    }

    @Override
    public RewardAccumulator transform(Purchase value) {

        RewardAccumulator rewardAccumulator = RewardAccumulator.builder(value).build();
        Integer stateValue = this.stateStore.get(rewardAccumulator.getCustomerId());

        if (stateValue != null) {
            rewardAccumulator.addRewardPoints(stateValue);
        }
        stateStore.put(rewardAccumulator.getCustomerId(), rewardAccumulator.getTotalRewardPoints());
        return rewardAccumulator;
    }

    @Override
    public void close() {

    }
}
