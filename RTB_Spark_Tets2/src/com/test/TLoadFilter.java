package com.test;
import org.apache.spark.sql.SparkSession;

public class TLoadFilter extends TRegLoadFilter {
    public void FilterAndSave(){
        super.FilterAndSave();
    };

    public TLoadFilter(String inWorkFolder, String inSaveFolderName, SparkSession inspark){
        super(inWorkFolder, inSaveFolderName, inspark, "SELECT _t, _p, device_type " +
                "FROM events " +
                "WHERE _n='app_loaded'");
        WorkFolder = inWorkFolder;
        SaveFolderName = inSaveFolderName;
        spark = inspark;

    }
}
