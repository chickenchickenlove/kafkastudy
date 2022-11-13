package kafkaStreams.chapter6;

import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import kafkaStreams.chapter6.processor.BeerPurchaseProcessor;
import kafkaStreams.domain.BeerPurchase;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.internals.KStreamPrint;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;

import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class Chapter6Stream {

    private final static String BEER_TOPIC = "pops-hops-purchases";


    public static void main(String[] args) {


        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());

        GsonSerializer<BeerPurchase> beerPurchaseGsonSerializer = new GsonSerializer<>();
        GsonDeserializer<BeerPurchase> beerPurchaseGsonDeserializer = new GsonDeserializer<>(BeerPurchase.class);

        Serde<BeerPurchase> beerPurchaseSerde = Serdes.serdeFrom(beerPurchaseGsonSerializer, beerPurchaseGsonDeserializer);
        Serde<String> stringSerde = Serdes.String();

        Topology topology = new Topology();


        String domesticSalesSink = "domestic-beer-sales";
        String internationalSalesSink = "international-beer-sales";
        String purchaseSourceNodeName = "beer-purchase-source";
        String purchaseProcessor = "purchase-processor";


        topology.addSource(EARLIEST,
                        purchaseSourceNodeName,
                        new UsePartitionTimeOnInvalidTimestamp(),
                        stringSerde.deserializer(),
                        beerPurchaseSerde.deserializer(),
                        BEER_TOPIC)
                .addProcessor(purchaseProcessor,
                        () -> new BeerPurchaseProcessor(domesticSalesSink, internationalSalesSink), // get() 메서드에서 항상 새로운 타입을 생성해야함.
                        purchaseSourceNodeName)
                .addSink(internationalSalesSink,
                        "international-sales",
                        stringSerde.serializer(),
                        beerPurchaseSerde.serializer(),
                        purchaseProcessor)
                .addSink(domesticSalesSink,
                        "domestic-sales",
                        stringSerde.serializer(),
                        beerPurchaseSerde.serializer(),
                        purchaseProcessor)
                .addProcessor("hello-1",
                        new KStreamPrint<String, BeerPurchase>((key, value) -> System.out.println("[hello-1] key = " + key + " value = " + value)),
                        purchaseProcessor)
                .addProcessor("hello-2",
                        new KStreamPrint<String, BeerPurchase>((key, value) -> System.out.println("[hello-2] key = " + key + " value = " + value)),
                        purchaseProcessor);


        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();


    }

}
