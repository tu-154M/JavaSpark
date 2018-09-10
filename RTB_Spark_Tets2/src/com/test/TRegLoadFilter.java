package com.test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;

public abstract class TRegLoadFilter implements iFilter {
    SparkSession spark;
    String WorkFolder;
    String SaveFolderName;
    String SQL;

    public TRegLoadFilter(String inWorkFolder, String inSaveFolderName, SparkSession inspark, String inSQL){
        WorkFolder = inWorkFolder;
        SaveFolderName = inSaveFolderName;
        spark = inspark;
        SQL = inSQL;
    }

    public void FilterAndSave(){
        Dataset<Row> event_app_loaded = spark.sql(SQL);
        //event_app_loaded.show();
        //удаление папок с паркет-файлами, если такие папки уже есть
        String FolderPath = WorkFolder+SaveFolderName;
        File ObjFolder = new File(FolderPath);
        if (ObjFolder.exists() && ObjFolder.isDirectory()) {
            File[] ArrFiles = ObjFolder.listFiles();
            for (File CurrFile:ArrFiles
            ) {
                CurrFile.delete();
            }
            ObjFolder.delete();
        };

        FolderPath = WorkFolder;
        ObjFolder = new File(FolderPath);
        if (!ObjFolder.exists()){
            ObjFolder.mkdirs();
        };
        //event_app_loaded.show();
        event_app_loaded.write().parquet(FolderPath+SaveFolderName);
    };


}
