package kafkaStreams.chapter5;

import kafkaStreams.chapter3.GsonDeserializer;
import kafkaStreams.chapter3.GsonSerializer;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.domain.TransactionSummary;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class KStreamTableJoin {

    public static final String STOCK_TRANSACTIONS_TOPIC = "stock-transactions";

    public static void main(String[] args) {


        GsonSerializer<StockTransaction> stockTransactionGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTransaction> stockTransactionGsonDeserializer = new GsonDeserializer<>(StockTransaction.class);
        GsonSerializer<TransactionSummary> transactionSummaryGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<TransactionSummary> transactionSummaryGsonDeserializer = new GsonDeserializer<>(TransactionSummary.class);

        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionGsonSerializer, stockTransactionGsonDeserializer);
        Serde<TransactionSummary> transactionSummarySerde = Serdes.serdeFrom(transactionSummaryGsonSerializer, transactionSummaryGsonDeserializer);
        Serde<String> stringSerde = Serdes.String();

        Properties props = new Properties();

        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        StreamsBuilder builder = new StreamsBuilder();

        KTable<Windowed<TransactionSummary>, Long> customerTransactionCounts = builder.stream(STOCK_TRANSACTIONS_TOPIC,
                        Consumed.with(stringSerde, stockTransactionSerde)
                                .withOffsetResetPolicy(EARLIEST))
                .groupBy((noKey, stockTransaction) -> TransactionSummary.from(stockTransaction)
                    , Grouped.with(transactionSummarySerde, stockTransactionSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(30)))
                .count();

        // countStream 생성
        KStream<String, TransactionSummary> countStream = customerTransactionCounts.toStream()
                .map((window, count) -> {
                    TransactionSummary transactionSummary = window.key();
                    String newKey = transactionSummary.getIndustry();
                    transactionSummary.setSummaryCount(count);
                    return KeyValue.pair(newKey, transactionSummary);
                });

        // table 생성
        KTable<String, String> financialNews = builder.
                table("financial-news", Consumed.with(stringSerde, stringSerde).withOffsetResetPolicy(EARLIEST));


        ValueJoiner<TransactionSummary, String, String> valueJoiner =
                (transactionSummary, news) ->
                        String.format("%d shares purchased %s related news [%s]",
                                        transactionSummary.getSummaryCount(), transactionSummary.getStockTicker(), news);


        KStream<String, String> joinedStream = countStream.join(financialNews, valueJoiner,
                Joined.with(stringSerde, transactionSummarySerde, stringSerde));

        joinedStream.print(Printed.<String, String>toSysOut().withLabel("Transactions and News"));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
    }

}
