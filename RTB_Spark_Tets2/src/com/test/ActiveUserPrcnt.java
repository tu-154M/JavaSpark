package com.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ActiveUserPrcnt implements CalcStat {
    SparkSession spark;
    Double result;
    Double getResult(){return result;};

    public  ActiveUserPrcnt(SparkSession inSpark){
        spark = inSpark;
    };

    public void calcStat(){
        int deltaSec = 7*24*60*60;//переводим 7 дней в секунды, чтобы сразу вычитать секунды без их преобразования в даты

        String sql ="SELECT load_cnt/all_cnt*100.00 prcn_active_p, load_cnt, all_cnt "+
                "FROM (" +
                        "SELECT COUNT(1) all_cnt, " +
                               "SUM(CASE WHEN min_load_date - reg_date BETWEEN 0 AND "+deltaSec+" THEN 1 ELSE 0 END) load_cnt " +
                        "FROM ( "+
                                "SELECT _p, " +
                                        "MIN(CASE WHEN _n='registered' THEN _t ELSE NULL END) reg_date, " +
                                        "MIN(CASE WHEN _n='app_loaded' THEN _t ELSE NULL END) min_load_date " +
                                  "FROM events " +
                                 "WHERE _n IN ('registered', 'app_loaded')" +
                                 "GROUP BY _p"+
                              ") WHERE reg_date IS NOT NULL" +
                ")"
                ;
        Dataset<Row> datasetCount = spark.sql(sql);

        //datasetCount.show();
        Row currRow =datasetCount.select("prcn_active_p").collectAsList().get(0);
        result= (Double) currRow.get(0);
    };
}
