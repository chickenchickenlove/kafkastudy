package kafkaStreams.client.producer;

public class BeerPurchaseProducer {

    public static void main(String[] args) {
        MockDataProducer.produceBeerPurchases(100);
    }

}
