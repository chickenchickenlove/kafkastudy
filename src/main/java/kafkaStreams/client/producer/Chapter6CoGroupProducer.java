package kafkaStreams.client.producer;

public class Chapter6CoGroupProducer {

    public static void main(String[] args) {
        MockDataProducer.produceStockTransactionsAndDayTradingClickEvents(50, 100, 100, stockTransaction -> stockTransaction.getSymbol());
    }

}
