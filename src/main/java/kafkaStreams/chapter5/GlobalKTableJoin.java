package kafkaStreams.chapter5;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.domain.TransactionSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import static kafkaStreams.util.Topics.CLIENTS;
import static kafkaStreams.util.Topics.COMPANIES;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

@Slf4j
public class GlobalKTableJoin {

    public static final String STOCK_TRANSACTIONS_TOPIC = "stock-transactions";

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props = InitProducerProperty.initProperty(props);


        GsonSerializer<StockTransaction> stockTransactionGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTransaction> stockTransactionGsonDeserializer = new GsonDeserializer<>(StockTransaction.class);

        GsonSerializer<TransactionSummary> transactionSummaryGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<TransactionSummary> transactionSummaryGsonDeserializer = new GsonDeserializer<>(TransactionSummary.class);

        Serde<TransactionSummary> transactionSummarySerde = Serdes.serdeFrom(transactionSummaryGsonSerializer, transactionSummaryGsonDeserializer);
        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionGsonSerializer, stockTransactionGsonDeserializer);
        Serde<String> stringSerde = Serdes.String();


        StreamsBuilder builder = new StreamsBuilder();
        KTable<Windowed<TransactionSummary>, Long> countTable = builder.stream(STOCK_TRANSACTIONS_TOPIC,
                        Consumed.with(stringSerde, stockTransactionSerde)
                                .withOffsetResetPolicy(EARLIEST))
                .groupBy((noKey, stockTransaction) -> TransactionSummary.from(stockTransaction),
                        Grouped.with(transactionSummarySerde, stockTransactionSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count();


        KStream<String, TransactionSummary> countStream = countTable.toStream().map((window, count) -> {
            TransactionSummary transactionSummary = window.key();
            String newKey = transactionSummary.getIndustry();
            transactionSummary.setSummaryCount(count);
            return KeyValue.pair(newKey, transactionSummary);
        });

        GlobalKTable<String, String> publicCompanies = builder.globalTable(COMPANIES.topicName(),
                Consumed.with(stringSerde, stringSerde));

        GlobalKTable<String, String> clients = builder.globalTable(CLIENTS.topicName(),
                Consumed.with(stringSerde, stringSerde));


        countStream.leftJoin(publicCompanies, (key, txn) -> txn.getStockTicker(),(readOnlyKey, value1, value2) -> value1.withCompanyName(value2))
                        .leftJoin(clients, (key, value) -> value.getCustomerId(), (readOnlyKey, value1, value2) -> value1.withCustomerName(value2))
                                .print(Printed.<String, TransactionSummary>toSysOut().withLabel("My Resolved"));


        Topology topology = builder.build();
        TopologyDescription describe = topology.describe();

        Set<TopologyDescription.Subtopology> subtopologies = describe.subtopologies();
        for (TopologyDescription.Subtopology subtopology : subtopologies) {
            System.out.println("subtopology = " + subtopology);
        }

        Set<TopologyDescription.GlobalStore> globalStores = describe.globalStores();
        for (TopologyDescription.GlobalStore globalStore : globalStores) {
            System.out.println("globalStore = " + globalStore);
        }

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();

    }





}
