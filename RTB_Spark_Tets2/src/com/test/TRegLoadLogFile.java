package com.test;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class TRegLoadLogFile extends TLogFile {
    private SparkSession spark;
    public SparkSession getSpark(){return spark;}

    public TRegLoadLogFile(String iWorkFolder, String iWorkFile){
        super(iWorkFolder, iWorkFile);
    }

    public void Init(){
        SparkConf conf = new SparkConf().setAppName("RTB_Spark_test2").setMaster("local[*]");
        spark = SparkSession
                .builder()
                .appName("RTB Java Spark SQL events test")
                .config(conf)
                .getOrCreate();
        //System.out.println("RTB Spark connected succesfully! "+ spark.toString());

        Dataset<Row> events = spark.read().json(getWorkFolder()+getWorkFile());
        events.createOrReplaceTempView("events");//withColumn("_t", from_unixtime("_t"))
        //events.printSchema();
        //System.out.println("Все события:");
        //events.show();
        //System.out.println("txt_rec_count="+events. .map(s -> 1).reduce((a, b) -> a + b).toString());
    };


}

