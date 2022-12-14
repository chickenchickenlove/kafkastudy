package ksqldbstudy.chapter5.timestamp;

import ksqldbstudy.chapter5.domain.Pulse;
import ksqldbstudy.chapter5.domain.Vital;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

public class PulseTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        String timestamp = ((Vital) record.value()).getTimestamp();

        SimpleDateFormat recv = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX", Locale.ENGLISH);
        SimpleDateFormat tran = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sssss", Locale.ENGLISH);

        String date;
        try {
            Date parse = recv.parse(timestamp);
            date = tran.format(parse);
        } catch (ParseException e) {
            return -1L;
        }

        return LocalDateTime.parse(date).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
