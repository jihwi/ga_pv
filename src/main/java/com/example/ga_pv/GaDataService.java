package com.example.ga_pv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Service
public class GaDataService {

    public Dataset<Row> getGaData() {
        SparkSession spark = SparkSession.builder()
                .appName("SparkExample")
                .master("local[*]")
                .getOrCreate();

        String parquetFilePath = "/Users/cjenm/Documents/23_07_ga/09-03-25fe3e5476274774b4ce744b2416a355.parquet";

        // Parquet 파일을 DataFrame으로 읽기
        Dataset<Row> df = spark.read().parquet(parquetFilePath);

        // DataFrame에서 쿼리 실행
        df.createOrReplaceTempView("parquetData"); // DataFrame을 테이블로 등록

        // 쿼리 실행 예시
        Dataset<Row> queryResult = spark.sql("SELECT cd_98_hit AS BD_CD, COUNT(cd_98_hit) AS PV " +
                "FROM parquetData " +
                "WHERE cd_98_hit IS NOT NULL AND type = 'pageview' GROUP BY cd_98_hit");

        queryResult.show();
        return queryResult;
    }
}
