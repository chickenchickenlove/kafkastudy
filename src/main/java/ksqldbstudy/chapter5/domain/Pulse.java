package ksqldbstudy.chapter5.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@Getter
@AllArgsConstructor
public class Pulse implements Vital{

    private String timeStamp;

    @Override
    public String getTimestamp() {
        return timeStamp;
    }
}
