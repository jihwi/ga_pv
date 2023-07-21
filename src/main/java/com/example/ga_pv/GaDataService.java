package com.example.ga_pv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

@Service
public class GaDataService {

    private final String basicDirectory = "/Users/cjenm/Documents/23_07_ga/";
    private String[] parquetPaths = {
            "09-03-25fe3e5476274774b4ce744b2416a355.parquet",
            "09-06-702fcb0c7a42425e98f68fc0ce7ea283.parquet",
            "09-10-3649c1eb2bcb431d9173408fe72d1f3c.parquet",
            "09-26-22e9ccc3a0dc4e5dbeb74cf6d7b13b31.parquet",
            "09-28-f608de2344a342b99d2a851f02b52f9c.parquet",
            "09-34-f87ad591c3b84e309f30959d099c00e3.parquet",
            "09-43-2b4c8594e8e942adbb7c6adb86e4580a.parquet",
            "09-47-241119c3063346abaa30891d60ee4167.parquet"
    };

    public Dataset<Row> getGaData() {
        SparkSession spark = SparkSession.builder()
                .appName("SparkExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataFrame = null;

        for (String parquetPath : parquetPaths) {
            String parquetFilePath = basicDirectory + parquetPath;

            // Parquet 파일을 DataFrame으로 읽기
            Dataset<Row> df = spark.read().parquet(parquetFilePath);

            if(dataFrame == null) {
                dataFrame = df;
            } else {
                dataFrame = dataFrame.union(df);
            }
        }

        //DataFrame API 사용
        Dataset<Row> queryResult = dataFrame.where(col("cd_98_hit").isNotNull().and(col("type").equalTo("pageview")))
                .groupBy("cd_98_hit")
                .agg(count("cd_98_hit").alias("PV"))
                .select(col("cd_98_hit").alias("BD_CD"), col("PV"));


        queryResult.show();
        return queryResult;
    }
}
