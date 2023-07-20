package com.example.ga_pv;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class PvController {

    private final PvService pvService;

    @RequestMapping("/test")
    public List<PvEntity> gaPvDbInsert() {
        return pvService.mysqlInsertTest();
    }
}
