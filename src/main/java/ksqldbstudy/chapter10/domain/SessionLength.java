package ksqldbstudy.chapter10.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SessionLength {
    private Long seasonId;
    private Long episodeCount;
}
