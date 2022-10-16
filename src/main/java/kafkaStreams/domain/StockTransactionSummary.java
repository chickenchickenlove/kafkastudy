package kafkaStreams.domain;


public class StockTransactionSummary {

    public double amount;
    public String tickerSymbol;
    public int sharesPurchased;
    public int sharesSold;
    private long lastUpdatedTime;



    public void update(StockTransaction transaction){
        this.amount += transaction.getSharePrice();
        if(transaction.getSector().equalsIgnoreCase("purchase")){
            this.sharesPurchased += transaction.getShares();
        } else{
            this.sharesSold += transaction.getShares();
        }
        this.lastUpdatedTime = System.currentTimeMillis();
    }

    public boolean updatedWithinLastMillis(long currentTime, long limit){
        return currentTime - this.lastUpdatedTime <= limit;
    }

    public static StockTransactionSummary fromTransaction(StockTransaction transaction){
        StockTransactionSummary summary = new StockTransactionSummary();
        summary.tickerSymbol = transaction.getSymbol();
        summary.update(transaction);
        return summary;
    }
}