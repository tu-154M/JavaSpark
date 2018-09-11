package com.test;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class RegLoadLogFile extends LogFile {
    private SparkSession spark;
    public SparkSession getSpark(){return spark;}

    public RegLoadLogFile(String inWorkFolder, String inWorkFile){
        super(inWorkFolder, inWorkFile);
    }

    public void init(){
        SparkConf conf = new SparkConf().setAppName("RTB_Spark_test2").setMaster("local[*]");
        spark = SparkSession
                .builder()
                .appName("RTB Java Spark SQL events test")
                .config(conf)
                .getOrCreate();
        //System.out.println("RTB Spark connected succesfully! "+ spark.toString());

        Dataset<Row> datasetEvents = spark.read().json(getWorkFolder()+getWorkFile());
        datasetEvents.createOrReplaceTempView("events");//withColumn("_t", from_unixtime("_t"))
        //datasetEvents.printSchema();
        //System.out.println("Все события:");
        //datasetEvents.show();
        //System.out.println("txt_rec_count="+datasetEvents. .map(s -> 1).reduce((a, b) -> a + b).toString());
    };


}

