package kafkaStreams.chapter7.staterestorelistener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MyStateRestoreListener implements StateRestoreListener {

    // 각 토픽 파티션 별로 복구해야할 offset을 기록한다.
    private Map<TopicPartition, Long> totalToRestore = new ConcurrentHashMap<>();

    // 각 토픽 파티션 별로 최종 복구된 offset를 기록한다.
    private Map<TopicPartition, Long> restoredSoFar = new ConcurrentHashMap<>();



    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        long toRestore = endingOffset - startingOffset;
        totalToRestore.put(topicPartition, toRestore);
        log.info("start restore. restore target : {}, topicPartition : {}, record to Restore : {}", storeName, topicPartition, toRestore);
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {

        // 복원된 전체 레코드 수 계산
        long currentProgress = batchEndOffset + restoredSoFar.getOrDefault(topicPartition, 0L);
        double percentComplete = (double) currentProgress / totalToRestore.get(topicPartition);
        log.info("restore progress : {}, progress Percent : {}, Target store : {}, topicPartition : {}",
                currentProgress, percentComplete, storeName, topicPartition );

        // 복원된 레코드 수를 저장한다.
        restoredSoFar.put(topicPartition, currentProgress);
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        log.info("Restore Completed : {}, topicPartition : {}", storeName, topicPartition);
    }
}
