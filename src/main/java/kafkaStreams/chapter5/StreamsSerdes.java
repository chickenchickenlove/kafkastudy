package kafkaStreams.chapter5;


import com.google.gson.reflect.TypeToken;
import kafkaStreams.util.FixedSizePriorityQueue;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import kafkaStreams.domain.*;
import kafkaStreams.util.Tuple;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class StreamsSerdes {

    public static Serde<PurchasePattern> PurchasePatternSerde() {
        return new PurchasePatternsSerde();
    }

    public static Serde<RewardAccumulator> RewardAccumulatorSerde() {
        return new RewardAccumulatorSerde();
    }

    public static Serde<Purchase> PurchaseSerde() {
        return new PurchaseSerde();
    }

    public static Serde<StockTickerData> StockTickerSerde() {
        return  new StockTickerSerde();
    }

    public static Serde<StockTransaction> StockTransactionSerde() {
        return new StockTransactionSerde();
    }

    public static Serde<FixedSizePriorityQueue> FixedSizePriorityQueueSerde() {
        return new FixedSizePriorityQueueSerde();
    }

    public static Serde<TransactionSummary> TransactionSummarySerde() {
        return new TransactionSummarySerde();
    }

    public static Serde<ShareVolume> ShareVolumeSerde() {
        return new ShareVolumeSerde();
    }

    public static Serde<StockPerformance> StockPerformanceSerde() {
        return new StockPerformanceSerde();
    }

    public static Serde<CustomerTransactions> CustomerTransactionsSerde() {
        return new CustomerTransactionsSerde();
    }

    public static Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> EventTransactionTupleSerde() {
        return new EventTransactionTupleSerde();
    }

    public static Serde<ClickEvent> ClickEventSerde() {
        return new ClickEventSerde();
    }

    public static Serde<List<ClickEvent>> EventListSerde() {
        return new EventsListSerde();
    }

    public static Serde<List<StockTransaction>> TransactionsListSerde() {
        return  new TransactionsListSerde();
    }


    public static Serde<PurchaseKey> purchaseKeySerde() {
        return new PurchaseKeySerde();
    }

    public static final class PurchaseKeySerde extends WrapperSerde<PurchaseKey> {
        public PurchaseKeySerde(){
            super(new GsonSerializer<>(), new GsonDeserializer<>(PurchaseKey.class) );
        }
    }

    public static final class PurchasePatternsSerde extends WrapperSerde<PurchasePattern> {
        public PurchasePatternsSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(PurchasePattern.class));
        }
    }

    public static final class RewardAccumulatorSerde extends WrapperSerde<RewardAccumulator> {
        public RewardAccumulatorSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(RewardAccumulator.class));
        }
    }

    public static final class PurchaseSerde extends WrapperSerde<Purchase> {
        public PurchaseSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(Purchase.class));
        }
    }

    public static final class StockTickerSerde extends WrapperSerde<StockTickerData> {
        public StockTickerSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(StockTickerData.class));
        }
    }

    public static final class StockTransactionSerde extends WrapperSerde<StockTransaction> {
        public StockTransactionSerde(){
            super(new GsonSerializer<>(), new GsonDeserializer<>(StockTransaction.class));
        }
    }

    public static final class CustomerTransactionsSerde extends WrapperSerde<CustomerTransactions> {
        public CustomerTransactionsSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(CustomerTransactions.class));
        }

    }

    public static final class FixedSizePriorityQueueSerde extends WrapperSerde<FixedSizePriorityQueue> {
        public FixedSizePriorityQueueSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(FixedSizePriorityQueue.class));
        }
    }

    public static final class ShareVolumeSerde extends WrapperSerde<ShareVolume> {
        public ShareVolumeSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(ShareVolume.class));
        }
    }

    public static final class TransactionSummarySerde extends WrapperSerde<TransactionSummary> {
        public TransactionSummarySerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(TransactionSummary.class));
        }
    }

    public static final class StockPerformanceSerde extends WrapperSerde<StockPerformance> {
        public StockPerformanceSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(StockPerformance.class));
        }
    }

    public static final class EventTransactionTupleSerde extends WrapperSerde<Tuple<List<ClickEvent>, List<StockTransaction>>> {
        private static final Type tupleType = new TypeToken<Tuple<List<ClickEvent>, List<StockTransaction>>>(){}.getType();
        public EventTransactionTupleSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(tupleType));
        }
    }

    public static final class ClickEventSerde extends WrapperSerde<ClickEvent> {
        public ClickEventSerde () {
            super(new GsonSerializer<>(), new GsonDeserializer<>(ClickEvent.class));
        }
    }

    public static final class TransactionsListSerde extends  WrapperSerde<List<StockTransaction>>  {
        private static final Type listType = new TypeToken<List<StockTransaction>>(){}.getType();
        public TransactionsListSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(listType));
        }
    }

    public static final class EventsListSerde extends  WrapperSerde<List<ClickEvent>>  {
        private static final Type listType = new TypeToken<List<ClickEvent>>(){}.getType();
        public EventsListSerde() {
            super(new GsonSerializer<>(), new GsonDeserializer<>(listType));
        }
    }

    private static class WrapperSerde<T> implements Serde<T> {

        private GsonSerializer<T> serializer;
        private GsonDeserializer<T> deserializer;

        WrapperSerde(GsonSerializer<T> serializer, GsonDeserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<T> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }}
