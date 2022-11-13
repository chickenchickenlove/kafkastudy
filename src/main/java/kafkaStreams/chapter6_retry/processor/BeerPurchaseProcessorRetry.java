package kafkaStreams.chapter6_retry.processor;

import kafkaStreams.domain.BeerPurchase;
import kafkaStreams.domain.Currency;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.text.DecimalFormat;

import static kafkaStreams.domain.Currency.DOLLARS;

public class BeerPurchaseProcessorRetry extends ContextualProcessor<String, BeerPurchase, String, BeerPurchase> {


    private final String domesticNode;
    private final String internationalNode;

    public BeerPurchaseProcessorRetry(String domesticNode, String internationalNode) {
        this.domesticNode = domesticNode;
        this.internationalNode = internationalNode;
    }

    @Override
    public void process(Record<String, BeerPurchase> beerPurchaseRecord) {

        BeerPurchase value = beerPurchaseRecord.value();
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

            // downStream
            super.context().forward(new Record<String, BeerPurchase>(beerPurchaseRecord.key(), dollarBeerPurchase, beerPurchaseRecord.timestamp()), internationalNode);
        }else{
            super.context().forward(beerPurchaseRecord, domesticNode);
        }
    }
}


