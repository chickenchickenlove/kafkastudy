package kafkaStreams.client.producer;

public class StartProducer {
    public static void main(String[] args) throws InterruptedException {


        while (true) {
            MockDataProducer.producePurchaseData(100, 100, 100);
            Thread.sleep(1000);
        }





    }
}
