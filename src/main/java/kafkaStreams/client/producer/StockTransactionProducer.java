package kafkaStreams.client.producer;

public class StockTransactionProducer {

    public static void main(String[] args) {
        MockDataProducer.produceStockTransactions(100, 5,5,false);
    }

}
