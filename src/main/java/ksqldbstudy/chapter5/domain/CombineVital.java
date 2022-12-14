package ksqldbstudy.chapter5.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CombineVital {

    private Long bpm;
    private BodyTemp bodyTemp;

}
