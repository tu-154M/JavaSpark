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
    }

    public void CalcStat() {
        int DeltaSec = 7*24*60*60;//количество секунд в одной неделе
        Dataset<Row> DSRegistered = spark.read().parquet(FilePathRegistered);
        DSRegistered.createOrReplaceTempView("v_registered");

        Dataset<Row> DSAppLoaded = spark.read().parquet(FilePathAppLoaded);
        DSAppLoaded.createOrReplaceTempView("v_app_loaded");

        String SQL = "SELECT load_cnt/reg_cnt*100.00 prcn_active_p, load_cnt, reg_cnt " +
                "FROM ( " +
                        "SELECT COUNT(DISTINCT r._p) reg_cnt, " +
                               //"SUM(CASE WHEN l._p IS NOT NULL THEN 1 ELSE 0 END) load_cnt " +
                               "COUNT(DISTINCT l._p) load_cnt " +
                          "FROM (SELECT r0._p, " +
                                       "unix_timestamp(next_day(from_unixtime(r0._t), 'Monday')) unix_next_mon " +
                                  "FROM v_registered r0" +
                               ") r " +
                          "LEFT OUTER JOIN ( " +
                                            "SELECT _p, " +
                                                   //"MIN(CAST(_t as bigint)) unix_min_load_date " +
                                                    "CAST(_t as bigint) unix_min_load_date " +
                                              "FROM v_app_loaded " +
                                             //"GROUP BY _p " +
                                          ") l " +
                            "ON r._p = l._p " +
                           "AND l.unix_min_load_date >= r.unix_next_mon  "  +
                           "AND l.unix_min_load_date < r.unix_next_mon + " + DeltaSec +
                ")";
        Dataset<Row> DSCount = spark.sql(SQL);

        //DSCount.show();
        Row CurrRow = DSCount.select("prcn_active_p").collectAsList().get(0);
        Result = (Double) CurrRow.get(0);
    }

    ;
}
