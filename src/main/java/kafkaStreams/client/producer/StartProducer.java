package kafkaStreams.client.producer;

public class StartProducer {
    public static void main(String[] args) {
        MockDataProducer.producePurchaseData(100, 100, 100);
    }
}
