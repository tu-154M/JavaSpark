package com.test;
import org.apache.spark.sql.SparkSession;

public class LoadFilter extends RegLoadFilter {
    public LoadFilter(String inWorkFolder, String inSaveFolderName, SparkSession inSpark){
        super(inWorkFolder, inSaveFolderName, inSpark, "SELECT _t, _p, device_type " +
                "FROM events " +
                "WHERE _n='app_loaded'");

    }
}
