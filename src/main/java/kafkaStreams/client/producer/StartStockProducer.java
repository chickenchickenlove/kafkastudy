package kafkaStreams.client.producer;

public class StartStockProducer {
    public static void main(String[] args) throws InterruptedException {
        MockDataProducer.produceStockTickerData(3, 1000);
    }
}
