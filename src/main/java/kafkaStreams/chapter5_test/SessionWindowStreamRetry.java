package kafkaStreams.chapter5_test;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.domain.TransactionSummary;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class SessionWindowStreamRetry {

    public static final String STOCK_TRANSACTIONS_TOPIC = "stock-transactions";

    public static void main(String[] args) {


        GsonSerializer<StockTransaction> stockTransactionGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTransaction> stockTransactionGsonDeserializer = new GsonDeserializer<>(StockTransaction.class);

        GsonSerializer<TransactionSummary> transactionSummaryGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<TransactionSummary> transactionSummaryGsonDeserializer = new GsonDeserializer<>(TransactionSummary.class);

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionGsonSerializer, stockTransactionGsonDeserializer);
        Serde<TransactionSummary> transactionSummarySerde = Serdes.serdeFrom(transactionSummaryGsonSerializer, transactionSummaryGsonDeserializer);


        Properties props = new Properties();

        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());


        StreamsBuilder builder = new StreamsBuilder();

        KTable<Windowed<TransactionSummary>, Long> countStream = builder.stream(STOCK_TRANSACTIONS_TOPIC,
                        Consumed.with(stringSerde, stockTransactionSerde)
                                .withOffsetResetPolicy(EARLIEST))
                .groupBy((key, stockTransaction) -> TransactionSummary.from(stockTransaction),
                        Grouped.with(transactionSummarySerde, stockTransactionSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(20)))
                .count();


        countStream.toStream().peek((key, value) -> System.out.println("key = " + key + " value = " + value));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
    }
}
