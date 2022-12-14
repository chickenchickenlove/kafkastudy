package kafkaStreamsAndKsqlDB.chapter3.domain;

import lombok.Data;

@Data
public class Tweet {

    private Long createdAt;
    private Long id;
    private String lang;
    private Boolean retweet;
    private String text;
}
