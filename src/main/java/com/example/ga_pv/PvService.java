package com.example.ga_pv;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class PvService {

    private final PvRepository pvRepository;
    private final GaDataService gaDataService;

    public List<PvEntity> mysqlInsertTest() {
        Dataset<Row> data = gaDataService.getGaData();

        List<PvEntity> pvEntities = new ArrayList<>();

        List<Row> rows = data.collectAsList();
        for (Row row : rows) {
            String bdCd = row.getString(0);
            long pv = row.getLong(1);
            log.debug("bdcd : {}, pv : {}", bdCd, pv);

            PvEntity pvEntity = new PvEntity();
            pvEntity.setBdCd(bdCd);
            pvEntity.setPv(pv);
            PvEntity save = pvRepository.save(pvEntity);
            pvEntities.add(save);
        }

        return pvEntities;
    }

}
