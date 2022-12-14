package ksqldbstudy.chapter5.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BodyTemp implements Vital{

    private String timeStamp;
    private Double temperature;
    private String unit;

    @Override
    public String getTimestamp() {
        return timeStamp;
    }
}
