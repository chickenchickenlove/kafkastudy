package kafkaStreams.client.producer;

import kafkaStreams.domain.StockTransaction;

public class StockTransactionAndNewsProducer1 {

    public static void main(String[] args) {
        MockDataProducer.produceStockTransactionsWithKeyFunction(50,50, 25, StockTransaction::getSymbol);

    }

}
