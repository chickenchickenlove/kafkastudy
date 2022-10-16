package kafkaStreams.chapter3;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafkaStreams.domain.Purchase;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class ZMartKafkaStreamsApp {

    public static void main(String[] args) {


        // 직렬화기 생성
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<Purchase>();
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<Purchase>(new ObjectMapper(), Purchase.class);
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);




    }


}
