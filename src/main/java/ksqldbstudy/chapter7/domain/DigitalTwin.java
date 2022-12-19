package ksqldbstudy.chapter7.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class DigitalTwin {

    private TurbineState desired;
    private TurbineState reported;

}
