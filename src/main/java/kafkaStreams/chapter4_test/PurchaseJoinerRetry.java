package kafkaStreams.chapter4_test;

import kafkaStreams.domain.CorrelatedPurchase;
import kafkaStreams.domain.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PurchaseJoinerRetry implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {
    @Override
    public CorrelatedPurchase apply(Purchase value1, Purchase value2) {

        String customerId1 = value1 != null ? value1.getCustomerId() : null;
        Double price1 = value1 != null ? value1.getPrice() : null;
        Date purchaseDate1 = value1 != null ? value1.getPurchaseDate() : null;
        String item1 = value1 != null ? value1.getItemPurchased() : null;

        String customerId2 = value2 != null ? value2.getCustomerId() : null;
        Double price2 = value2 != null ? value2.getPrice() : null;
        Date purchaseDate2 = value2 != null ? value2.getPurchaseDate() : null;
        String item2 = value2!= null ? value2.getItemPurchased() : null;

        List<String> itemList = new ArrayList<>();

        if (item1 != null) {
            itemList.add(item1);
        }

        if (item2 != null) {
            itemList.add(item2);
        }

        CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();
        CorrelatedPurchase ret = builder
                .withTotalAmount(price1 + price2)
                .withItemsPurchased(itemList)
                .withFirstPurchaseDate(purchaseDate1)
                .withSecondPurchaseDate(purchaseDate2)
                .withCustomerId(customerId1 != null ? customerId1 : customerId2).build();

        return ret;
    }
}
