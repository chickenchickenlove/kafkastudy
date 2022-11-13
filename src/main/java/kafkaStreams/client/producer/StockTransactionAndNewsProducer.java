package kafkaStreams.client.producer;

public class StockTransactionAndNewsProducer {

    public static void main(String[] args) {
        MockDataProducer.produceStockTransactions(100, 50, 50, true);

    }

}
