package kafkaStreamsAndKsqlDB.chapter3;

import kafkaStreamsAndKsqlDB.chapter3.customSerde.CustomGsonDeserializer;
import kafkaStreamsAndKsqlDB.chapter3.customSerde.CustomGsonSerializer;
import kafkaStreamsAndKsqlDB.chapter3.domain.Tweet;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;

public class CryptoTopology {

    public static void main(String[] args) {


        CustomGsonSerializer<Tweet> tweetCustomGsonSerializer = new CustomGsonSerializer<>();
        CustomGsonDeserializer<Tweet> tweetCustomGsonDeserializer = new CustomGsonDeserializer<>(Tweet.class);
        Serde<Tweet> tweetSerde = Serdes.serdeFrom(tweetCustomGsonSerializer, tweetCustomGsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Tweet> tweetStream = builder.stream("tweets",
                Consumed.with(Serdes.String(), tweetSerde));

        KStream<String, Tweet> filteredStream = tweetStream.filterNot((key, value) -> value.getRetweet());

        Map<String, KStream<String, Tweet>> branched = filteredStream.split(Named.as("branched-"))
                .branch((key, value) -> value.getLang().equals("en"), Branched.as("english"))
                .branch((key, value) -> !value.getLang().equals("en"), Branched.as("non-english"))
                .noDefaultBranch();

        KStream<String, Tweet> english = branched.get("branched-english");
        KStream<String, Tweet> nonEnglish = branched.get("branched-non-english");

        KStream<String, Tweet> translatedStream = nonEnglish.mapValues((readOnlyKey, value) -> {
            value.setText("translate" + value.getText());
            return value;
        });


        KStream<String, Tweet> mergedStream = english.merge(translatedStream);


    }


}

