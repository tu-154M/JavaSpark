package com.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TActiveUserPrcntByParquet implements  iCalcStat {
    SparkSession spark;
    Double Result;
    String FilePathRegistered;
    String FilePathAppLoaded;

    Double getResult() {
        return Result;
    } ;

    public TActiveUserPrcntByParquet(String inFilePathRegistered, String inFilePathAppLoaded, SparkSession inspark) {
        FilePathRegistered = inFilePathRegistered;
        FilePathAppLoaded = inFilePathAppLoaded;
        spark = inspark;
    };

    public void CalcStat() {
        int DeltaSec = 7 * 24 * 60 * 60;//переводим 7 дней в секунды, чтобы сразу вычитать секунды без их преобразования в даты

        Dataset<Row> DSRegistered = spark.read().parquet(FilePathRegistered);
        DSRegistered.createOrReplaceTempView("v_registered");

        Dataset<Row> DSAppLoaded = spark.read().parquet(FilePathAppLoaded);
        DSAppLoaded.createOrReplaceTempView("v_app_loaded");

        String SQL = "SELECT load_cnt/all_cnt*100.00 prcn_active_p, load_cnt, all_cnt " +
                "FROM ( " +
                        "SELECT COUNT(1) all_cnt, " +
                               "SUM(CASE WHEN l._p IS NOT NULL THEN 1 ELSE 0 END) load_cnt " +
                          "FROM v_registered r " +
                          "LEFT OUTER JOIN ( " +
                                            "SELECT _p, " +
                                                   "MIN(_t) min_load_date " +
                                              "FROM v_app_loaded " +
                                             "GROUP BY _p " +
                                          ") l " +
                            "ON r._p = l._p AND l.min_load_date - r._t BETWEEN 0 AND " + DeltaSec +
                ")";
        Dataset<Row> DSCount = spark.sql(SQL);

        //DSCount.show();
        Row CurrRow = DSCount.select("prcn_active_p").collectAsList().get(0);
        Result = (Double) CurrRow.get(0);
    }

    ;
}
