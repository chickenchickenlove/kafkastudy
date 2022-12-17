package ksqldbstudy.chapter11.domain;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class NetflixSession {

    private int sessionId;
    private int titleId;
    private Date createdAt;

}
