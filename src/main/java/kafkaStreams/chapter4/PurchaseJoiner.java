package kafkaStreams.chapter4;

import kafkaStreams.domain.CorrelatedPurchase;
import kafkaStreams.domain.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Date;

public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {
    @Override
    public CorrelatedPurchase apply(Purchase purchase1, Purchase purchase2) {

        CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();

        String customerId1 = purchase1 != null ? purchase1.getCustomerId() : null;
        Date purchaseDate1 = purchase1 != null ? purchase1.getPurchaseDate() : null;
        Double purchasePrice1 = purchase1 != null ? purchase1.getPrice() : null;
        String itemPurchase1 = purchase1 != null ? purchase1.getItemPurchased() : null;

        String customerId2 = purchase2 != null ? purchase2.getCustomerId() : null;
        Date purchaseDate2 = purchase2 != null ? purchase2.getPurchaseDate() : null;
        Double purchasePrice2 = purchase2 != null ? purchase2.getPrice() : null;
        String itemPurchase2 = purchase2 != null ? purchase2.getItemPurchased() : null;

        ArrayList<String> purchasedItem = new ArrayList<>();

        if (itemPurchase1 != null) {
            purchasedItem.add(itemPurchase1);
        }

        if (itemPurchase2 != null) {
            purchasedItem.add(itemPurchase2);
        }

        builder
                .withCustomerId(customerId1 != null ? customerId1 : customerId2)
                .withFirstPurchaseDate(purchaseDate1)
                .withSecondPurchaseDate(purchaseDate2)
                .withItemsPurchased(purchasedItem)
                .withTotalAmount(purchasePrice1 + purchasePrice2);

        return builder.build();
    }
}

