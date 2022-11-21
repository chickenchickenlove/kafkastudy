package chaper8;

import kafkaStreams.chapter5.StreamsSerdes;
import kafkaStreams.chapter6.processor.MyStockTransactionProcessorWithPunctuator;
import kafkaStreams.domain.StockTransaction;
import kafkaStreams.util.DataGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

public class StockPerformanceTransformerTest {

    private ContextualProcessor processorUnderTest;
    private MockProcessorContext mockProcessorContext;
    private final String STORE_NAME = "HELLO";


    @Before
    public void setUP() {

        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // MockProcessor 생성
        mockProcessorContext = new MockProcessorContext<>(props);

        // stateStore 생성
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(STORE_NAME);
        StoreBuilder<KeyValueStore<String, StockTransaction>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), StreamsSerdes.StockTransactionSerde());
        KeyValueStore<String, StockTransaction> stateStore = keyValueStoreStoreBuilder.withLoggingDisabled().withCachingDisabled().build();

        // stateStore 추가 및 초기화
        mockProcessorContext.addStateStore(stateStore);
        org.apache.kafka.streams.processor.MockProcessorContext mockProcessorContext1 = new org.apache.kafka.streams.processor.MockProcessorContext();
        stateStore.init(mockProcessorContext1, stateStore);
        processorUnderTest = new MyStockTransactionProcessorWithPunctuator(STORE_NAME);

        // 초기화
        processorUnderTest.init(mockProcessorContext);
    }

    @Test
    @DisplayName("process")
    public void test2() {
        StockTransaction stockTransaction = DataGenerator.generateStockTransaction();
        processorUnderTest.process(new Record(stockTransaction.getSymbol(), stockTransaction, SystemTime.SYSTEM.milliseconds()));
        KeyValueStore<String, StockTransaction> stateStore = (KeyValueStore<String, StockTransaction>) mockProcessorContext.getStateStore(STORE_NAME);
        StockTransaction forwarded = stateStore.get("[NEXT-KEY]" + stockTransaction.getSymbol());

        assertThat(mockProcessorContext.forwarded().size()).isEqualTo(0);
        assertThat(forwarded.getCustomerId()).isEqualTo(stockTransaction.getCustomerId());
        assertThat(forwarded.getSector()).isEqualTo(stockTransaction.getSector());

        mockProcessorContext.resetForwards();
        assertThat(mockProcessorContext.forwarded().size()).isEqualTo(0);
    }

    @Test
    @DisplayName("punctuate")
    public void test3() {
        StockTransaction stockTransaction = DataGenerator.generateStockTransaction();
        processorUnderTest.process(new Record(stockTransaction.getSymbol(), stockTransaction, SystemTime.SYSTEM.milliseconds()));

        MockProcessorContext.CapturedPunctuator capturedPunctuator = (MockProcessorContext.CapturedPunctuator)
                mockProcessorContext.scheduledPunctuators().get(0);

        Duration interval = capturedPunctuator.getInterval();
        PunctuationType type = capturedPunctuator.getType();


        Iterator iteratorBeforePunctuate = mockProcessorContext.forwarded().iterator();
        assertThat(iteratorBeforePunctuate.hasNext()).isFalse();

        // punctuate를 실제로 진행해야지 forwardedContext에 들어가있는다.
        Punctuator punctuator = capturedPunctuator.getPunctuator();
        punctuator.punctuate(0L);

        List<MockProcessorContext.CapturedForward<String,StockTransaction >> forwarded = mockProcessorContext.forwarded();
        Iterator<MockProcessorContext.CapturedForward<String, StockTransaction>> iteratorAfterPunctuate = forwarded.iterator();
        assertThat(iteratorAfterPunctuate.hasNext()).isTrue();

        Record<String, StockTransaction> record = iteratorAfterPunctuate.next().record();
        assertThat(record.key()).isEqualTo("[NEXT-KEY]" + stockTransaction.getSymbol());
        assertThat(record.value().getCustomerId()).isEqualTo(stockTransaction.getCustomerId());
    }


}