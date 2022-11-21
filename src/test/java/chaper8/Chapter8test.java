package chaper8;


import kafkaStreams.chapter5.StreamsSerdes;
import kafkaStreams.chapter8.StockPerformanceStreamsProcessorTopology;
import kafkaStreams.domain.StockPerformance;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.DataGenerator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

public class Chapter8test {

    private TestInputTopic<String, StockTransaction> inputTopic;
    private KeyValueStore<String, StockPerformance> keyValueStore;

    @Before
    public void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "ks-papi-stock-analysis-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ks-papi-stock-analysis-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-stock-analysis-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(StockPerformanceStreamsProcessorTopology.build(), props);
        inputTopic = topologyTestDriver.createInputTopic("stock-transactions",Serdes.String().serializer(), StreamsSerdes.StockTransactionSerde().serializer());
        keyValueStore = topologyTestDriver.getKeyValueStore("stock-performance-store");

    }

    @Test
    public void test1() {
        StockTransaction stockTransaction = DataGenerator.generateStockTransaction();
        StockTransaction stockTransaction1 = DataGenerator.generateStockTransaction();
        inputTopic.pipeInput(stockTransaction.getSymbol(),stockTransaction);
        inputTopic.pipeInput(stockTransaction.getSymbol(),stockTransaction1);

        StockPerformance stockPerformance = keyValueStore.get(stockTransaction.getSymbol());
        assertThat(stockPerformance.getCurrentShareVolume()).isEqualTo(stockTransaction.getShares());
        assertThat(stockPerformance.getCurrentPrice()).isEqualTo(stockTransaction.getSharePrice());


    }



}
