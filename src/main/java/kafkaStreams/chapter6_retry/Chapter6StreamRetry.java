package kafkaStreams.chapter6_retry;

import kafkaStreams.chapter6_retry.processor.BeerPurchaseProcessorRetry;
import kafkaStreams.domain.BeerPurchase;
import kafkaStreams.util.GsonDeserializer;
import kafkaStreams.util.GsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.internals.KStreamPrint;

import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class Chapter6StreamRetry {


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
        String internationalNodeName = "international-node";
        String domesticNodeName = "domestic-node";
        String internationalSalesSink = "international-beer-sales";
        String purchaseSourceNodeName = "beer-purchase-source";
        String purchaseProcessor = "purchase-processor";

        topology
                .addSource(EARLIEST,
                        purchaseSourceNodeName,
                        stringSerde.deserializer(),
                        beerPurchaseSerde.deserializer(),
                        BEER_TOPIC)
                .addProcessor(purchaseProcessor,
                        () -> new BeerPurchaseProcessorRetry(internationalNodeName, domesticNodeName),
                        purchaseSourceNodeName)
                .addProcessor(internationalNodeName,
                        new KStreamPrint<>((key, value) -> System.out.println("[INTERNATIONAL] key = " + key + " value = " + value)),
                        purchaseProcessor)
                .addProcessor(domesticNodeName,
                        new KStreamPrint<>((key, value) -> System.out.println("[DOMESTIC] key = " + key + " value = " + value)),
                        purchaseProcessor);


        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
    }


}
