package com.test;
import org.apache.spark.sql.SparkSession;

public class TRegFilter extends TRegLoadFilter {
    public void FilterAndSave(){
        super.FilterAndSave();
    };

    public TRegFilter(String inWorkFolder, String inSaveFolderName, SparkSession inspark){
        super(inWorkFolder, inSaveFolderName, inspark, "SELECT _t, _p, channel " +
                "FROM events " +
                "WHERE _n='registered'");
        WorkFolder = inWorkFolder;
        SaveFolderName = inSaveFolderName;
        spark = inspark;

    }
}