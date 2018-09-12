package com.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ActiveUserPrcntByParquet implements CalcStat {
    SparkSession spark;
    Double result;
    String filePathRegistered;
    String filePathAppLoaded;

    Double getResult() {
        return result;
    } ;

    public ActiveUserPrcntByParquet(String inFilePathRegistered, String inFilePathAppLoaded, SparkSession inSpark) {
        filePathRegistered = inFilePathRegistered;
        filePathAppLoaded = inFilePathAppLoaded;
        spark = inSpark;
    }

    public void calcStat() {
        int deltaSecOneWeek = 7*24*60*60;//количество секунд в одной неделе
        Dataset<Row> datasetRegistered = spark.read().parquet(filePathRegistered);
        datasetRegistered.createOrReplaceTempView("v_registered");
        datasetRegistered = null;

        Dataset<Row> datasetAppLoaded = spark.read().parquet(filePathAppLoaded);
        datasetAppLoaded.createOrReplaceTempView("v_app_loaded");
        datasetAppLoaded = null;

        String sql = "SELECT load_cnt/reg_cnt*100.00 prcn_active_p, load_cnt, reg_cnt " +
                "FROM ( " +
                        "SELECT COUNT(DISTINCT r._p) reg_cnt, " +
                               "COUNT(DISTINCT l._p) load_cnt " +
                          "FROM v_registered r " +
                          "LEFT OUTER JOIN v_app_loaded l " +
                            "ON r._p = l._p " +
                           "AND CAST(l._t as bigint) >= unix_timestamp(next_day(from_unixtime(r._t), 'Monday'))  "  +
                           "AND CAST(l._t as bigint) <  unix_timestamp(next_day(from_unixtime(r._t), 'Monday')) + " + deltaSecOneWeek +
                     ")";
        Dataset<Row> datasetCount = spark.sql(sql);

        //datasetCount.show();
        Row currRow = datasetCount.select("prcn_active_p").collectAsList().get(0);
        result = (Double) currRow.get(0);
    }

    ;
}
