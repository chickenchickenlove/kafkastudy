package kafkaStreams.chapter6.processor;

import kafkaStreams.domain.BeerPurchase;
import kafkaStreams.domain.Currency;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.text.DecimalFormat;

import static kafkaStreams.domain.Currency.DOLLARS;

public class BeerPurchaseProcessor extends ContextualProcessor<String, BeerPurchase, String, BeerPurchase> {

    private String domesticSalesNode;
    private String internationalSalesNode;

    public BeerPurchaseProcessor(String domesticSalesNode, String internationalSalesNode) {
        this.domesticSalesNode = domesticSalesNode;
        this.internationalSalesNode = internationalSalesNode;
    }

    @Override
    public void process(Record<String, BeerPurchase> record) {

        BeerPurchase value = record.value();
        Currency transactionCurrency = value.getCurrency();

        if (transactionCurrency != DOLLARS) {
            BeerPurchase dollarBeerPurchase;

            BeerPurchase.Builder builder = BeerPurchase.newBuilder(value);
            double internationalSaleAmount = value.getTotalSale();
            String pattern = "###.##";
            DecimalFormat decimalFormat = new DecimalFormat(pattern);
            builder.currency(DOLLARS);
            builder.totalSale(Double.parseDouble(decimalFormat.format(transactionCurrency.convertToDollars(internationalSaleAmount))));
            dollarBeerPurchase = builder.build();

            Record<String, BeerPurchase> nextRecord = new Record<>(record.key(), dollarBeerPurchase, record.timestamp(), record.headers());
            super.context().forward(nextRecord, "hello-1");
        }else{
            super.context().forward(record, "hello-2");
        }
    }
}
