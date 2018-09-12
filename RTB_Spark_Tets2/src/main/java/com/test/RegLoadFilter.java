package com.test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;

public abstract class RegLoadFilter implements Filter {
    SparkSession spark;
    String workFolder;
    String saveFolderName;
    String sql;

    public RegLoadFilter(String inWorkFolder, String inSaveFolderName, SparkSession inspark, String inSQL){
        workFolder = inWorkFolder;
        saveFolderName = inSaveFolderName;
        spark = inspark;
        sql = inSQL;
    }

    public void filterAndSave(){
        Dataset<Row> datasetRegLoad = spark.sql(sql);
        //datasetRegLoad.show();
        //удаление папок с паркет-файлами, если такие папки уже есть
        String folderPath = workFolder+saveFolderName;
        File objFolder = new File(folderPath);
        if (objFolder.exists() && objFolder.isDirectory()) {
            File[] ArrFiles = objFolder.listFiles();
            for (File currFile:ArrFiles
            ) {
                currFile.delete();
            }
            objFolder.delete();
        };

        folderPath = workFolder;
        objFolder = new File(folderPath);
        if (!objFolder.exists()){
            objFolder.mkdirs();
        };
        //event_app_loaded.show();
        datasetRegLoad.write().parquet(folderPath+saveFolderName);
    };


}
