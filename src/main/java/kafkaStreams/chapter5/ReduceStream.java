package kafkaStreams.chapter5;

import kafkaStreams.util.FixedSizePriorityQueue;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import kafkaStreams.domain.ShareVolume;
import kafkaStreams.domain.StockTransaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

@Slf4j
public class ReduceStream {

    public static final String STOCK_TRANSACTIONS_TOPIC = "stock-transactions";

    public static void main(String[] args) {


        Comparator<ShareVolume> shareVolumeComparator = (sv1, sv2) -> sv2.getShares() - sv1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedQue = new FixedSizePriorityQueue<ShareVolume>(shareVolumeComparator, 5);

        GsonSerializer<StockTransaction> stockTransactionGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<StockTransaction> stockTransactionGsonDeserializer = new GsonDeserializer<>(StockTransaction.class);

        GsonSerializer<ShareVolume> shareVolumeGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<ShareVolume> shareVolumeGsonDeserializer = new GsonDeserializer<>(ShareVolume.class);

        GsonSerializer<FixedSizePriorityQueue> fixedSizePriorityQueueGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<FixedSizePriorityQueue> fixedSizePriorityQueueGsonDeserializer = new GsonDeserializer<>(FixedSizePriorityQueue.class);


        Serde<StockTransaction> stockTransactionSerde = Serdes.serdeFrom(stockTransactionGsonSerializer, stockTransactionGsonDeserializer);
        Serde<ShareVolume> shareVolumeSerde = Serdes.serdeFrom(shareVolumeGsonSerializer, shareVolumeGsonDeserializer);
        Serde<String> stringSerde = Serdes.String();
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = Serdes.serdeFrom(fixedSizePriorityQueueGsonSerializer, fixedSizePriorityQueueGsonDeserializer);


        Properties props = new Properties();

        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, StockTransaction> sourceStream = builder.stream(STOCK_TRANSACTIONS_TOPIC,
                Consumed.with(stringSerde, stockTransactionSerde).withOffsetResetPolicy(EARLIEST)).peek((key, value) -> System.out.println("value = " + value));

        // ?????????????????? ??? ?????? ??????????????? Internal Topic??? ?????? ??? ?????????.
        // ????????? Broker?????? Log??? ??????????????? ?????????. ????????? Key / Value Serde??? ????????????.
        KGroupedStream<String, ShareVolume> stringShareVolumeKGroupedStream = sourceStream
                .mapValues(value -> ShareVolume.newBuilder(value).build())
                .groupBy((key, value) -> value.getSymbol(),
                        Grouped.<String, ShareVolume>keySerde(stringSerde).withValueSerde(shareVolumeSerde));

        // ?????? Key??? Value??? ??????, ??? ?????? Value??? ???????????? ?????? ???????????? ????????? ????????? ????????? ??????.
        KTable<String, ShareVolume> shareVolume = stringShareVolumeKGroupedStream.reduce(ShareVolume::sum,
                Materialized.<String, ShareVolume, KeyValueStore<Bytes, byte[]>>as("" +
                        "hello"));

        NumberFormat numberFormat = NumberFormat.getInstance();

        ValueMapper<FixedSizePriorityQueue, String> valueMapper = fpq -> {
            StringBuilder stringBuilder = new StringBuilder();
            Iterator<ShareVolume> iterator = fpq.iterator();
            int counter = 1;

            while (iterator.hasNext()) {
                ShareVolume stockVolume = iterator.next();
                if (stockVolume != null) {
                    stringBuilder
                            .append(counter++)
                            .append(")")
                            .append(stockVolume.getSymbol())
                            .append(":")
                            .append(numberFormat.format(stockVolume.getShares()))
                            .append(" ");
                }
            }
            return builder.toString();
        };

        shareVolume.groupBy((key, value) -> KeyValue.pair(value.getIndustry(), value),
                        Grouped.with(stringSerde, shareVolumeSerde))
                .aggregate(
                        () -> new FixedSizePriorityQueue<>(shareVolumeComparator, 5),
                        (key, value, aggregate) -> aggregate.add(value),
                        (key, value, aggregate) -> aggregate.remove(value),
                        Materialized.with(stringSerde, fixedSizePriorityQueueSerde))
                .mapValues(valueMapper)
                .toStream().peek((key, value) -> log.info("Stock Volume by industry {} {}", key, value))
                .to("stock-volume-by-company",
                        Produced.with(stringSerde, stringSerde));



//        shareVolumeKTable.toStream().print(Printed.<Stringing, ShareVolume>toSysOut().withLabel("KTABLE"));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();


    }

}
