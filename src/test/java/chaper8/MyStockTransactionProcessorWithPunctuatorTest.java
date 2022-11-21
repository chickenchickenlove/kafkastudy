package chaper8;


import kafkaStreams.chapter5.StreamsSerdes;
import kafkaStreams.chapter8.MyProcessorWithPunctuator;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.DataGenerator;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.scala.Serdes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.processor.PunctuationType.STREAM_TIME;
import static org.assertj.core.api.Assertions.*;

public class MyStockTransactionProcessorWithPunctuatorTest {

    private final String STORE_NAME = "HELLO";
    private Processor myProcessor;
    private MockProcessorContext<String, StockTransaction> processorContext;


    @Before
    public void setUp() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        processorContext = new MockProcessorContext<>(props);

        // StateStore 생성
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(STORE_NAME);
        StoreBuilder<KeyValueStore<String, StockTransaction>> stateStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), StreamsSerdes.StockTransactionSerde());
        KeyValueStore<String, StockTransaction> stateStore = stateStoreBuilder.withCachingDisabled().withLoggingDisabled().build();

        stateStore.init(processorContext.getStateStoreContext(), stateStore);
        processorContext.addStateStore(stateStore);

        myProcessor = new MyProcessorWithPunctuator(STORE_NAME);
        myProcessor.init(processorContext);
    }

    @Test
    @DisplayName("process test")
    public void test1() {
        StockTransaction stockTransaction = DataGenerator.generateStockTransaction();
        myProcessor.process(new Record(stockTransaction.getSymbol(), stockTransaction, SystemTime.SYSTEM.milliseconds()));
        assertThat(processorContext.forwarded().size()).isEqualTo(1);
        assertThat(processorContext.forwarded().get(0).record().key()).isEqualTo("new-" + stockTransaction.getSymbol());
        assertThat(processorContext.forwarded().get(0).record().value().getCustomerId()).isEqualTo(stockTransaction.getCustomerId());
    }

    @Test
    @DisplayName("punctuate test")
    public void test2() {
        StockTransaction stockTransaction = DataGenerator.generateStockTransaction();
        myProcessor.process(new Record(stockTransaction.getSymbol(), stockTransaction, SystemTime.SYSTEM.milliseconds()));

        List<MockProcessorContext.CapturedPunctuator> capturedPunctuators = processorContext.scheduledPunctuators();
        MockProcessorContext.CapturedPunctuator capturedPunctuator = capturedPunctuators.get(0);

        processorContext.resetForwards();

        Duration interval = capturedPunctuator.getInterval();
        PunctuationType type = capturedPunctuator.getType();
        Punctuator punctuator = capturedPunctuator.getPunctuator();
        punctuator.punctuate(0);

        assertThat(interval).isEqualTo(Duration.ofSeconds(15));
        assertThat(type).isEqualTo(STREAM_TIME);

        MockProcessorContext.CapturedForward<? extends String, ? extends StockTransaction> capturedForward = processorContext.forwarded().get(0);
        Record<? extends String, ? extends StockTransaction> record = capturedForward.record();

        assertThat(processorContext.forwarded().size()).isEqualTo(1);
        assertThat(record.key()).isEqualTo(stockTransaction.getSymbol());
    }



}
