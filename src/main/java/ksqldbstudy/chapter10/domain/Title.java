package ksqldbstudy.chapter10.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Title {

    private Long id;
    private String title;
    private Boolean onSchedule;
}
