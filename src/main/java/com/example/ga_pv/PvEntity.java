package com.example.ga_pv;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@Getter @Setter
public class PvEntity {
    @Id
    private String bdCd;

    private Long pv;

}
