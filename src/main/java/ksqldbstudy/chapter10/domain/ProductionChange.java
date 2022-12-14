package ksqldbstudy.chapter10.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDate;
import java.util.Date;

@AllArgsConstructor
@Data
public class ProductionChange {

    private String uuid;
    private Long titleId;
    private String changeType;
    private SessionLength before;
    private SessionLength after;
    private Date date;




}
