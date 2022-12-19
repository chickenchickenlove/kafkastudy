package ksqldbstudy.chapter7.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class TurbineState {

    private String timestamp;
    private Double windSpeedMph;

    private Power power;
    private Type type;


    public TurbineState clone() {
        return new TurbineState(this.timestamp, this.windSpeedMph, this.power, this.type);
    }



}






