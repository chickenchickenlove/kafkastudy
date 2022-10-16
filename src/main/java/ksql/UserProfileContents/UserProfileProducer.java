package ksql.UserProfileContents;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;

public class UserProfileProducer {

    private static List<String> firstNameList = List.of("Alice","Bob","Carol","Dan","Eve","Frank","Grace","Heidi","Ivan");
    private static  List<String> lastNameList = List.of("Smith","Jones","Coen","Fawcett","Edison","Jones","Dotty");
    private static  List<String> countryCodeList = List.of("AU","IN","GB","US");
    private static  List<String> ratingList = List.of("3.4","3.9","2.2","4.4","3.7","4.9");


    private static int getIndex(int num) {
        return new Random().nextInt(num -1 );
    }



    public static void main(String[] args) throws InterruptedException {

        String topicName = "USERPROFILE";

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserProfileSerializer.class.getName());

        KafkaProducer<String, UserProfile> producer = new KafkaProducer<String, UserProfile>(props);

        int i = 0;
        while (true) {

            UserProfile userProfile = new UserProfile();
            userProfile.setUserId(String.valueOf(i));
            userProfile.setFirstName(firstNameList.get(getIndex(firstNameList.size())));
            userProfile.setLastName(lastNameList.get(getIndex(lastNameList.size())));
            userProfile.setCountryCode(countryCodeList.get(getIndex(countryCodeList.size())));
            userProfile.setRating(ratingList.get(getIndex(ratingList.size())));
            userProfile.setTs(System.currentTimeMillis());

            ProducerRecord<String, UserProfile> producerRecord = new ProducerRecord<String, UserProfile>(topicName, userProfile);
            producer.send(producerRecord);

            Thread.sleep(1000);
            System.out.println("i = " + i);
            i++;
        }

    }


}
