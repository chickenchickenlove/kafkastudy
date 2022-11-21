package kafkaStreams.chapter8;

import kafkaStreams.chapter5.StreamsSerdes;
import kafkaStreams.domain.StockPerformance;
import kafkaStreams.domain.StockTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class StockPerformanceStreamsProcessorTopology {

    public static Topology build() {

        Serde<String> stringSerde = Serdes.String();
        Serde<StockPerformance> stockPerformanceSerde = StreamsSerdes.StockPerformanceSerde();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();


        StreamsBuilder builder = new StreamsBuilder();

        String stocksStateStore = "stock-performance-store";
        double differentialThreshold = 0.02;

        KeyValueBytesStoreSupplier storeSupplier = Stores.lruMap(stocksStateStore, 100);
        StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), stockPerformanceSerde);

        builder.addStateStore(storeBuilder);

        builder.stream("stock-transactions", Consumed.with(stringSerde, stockTransactionSerde))
                .transform(() -> new StockPerformanceTransformer(stocksStateStore, differentialThreshold), stocksStateStore)
                .to("stock-performance", Produced.with(stringSerde, stockPerformanceSerde));

        return builder.build();
    }

}
