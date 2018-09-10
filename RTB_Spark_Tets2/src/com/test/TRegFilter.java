package com.test;
import org.apache.spark.sql.SparkSession;

public class TRegFilter extends TRegLoadFilter {

    public TRegFilter(String inWorkFolder, String inSaveFolderName, SparkSession inspark){
        super(inWorkFolder, inSaveFolderName, inspark, "SELECT _t, _p, channel " +
                /*", CAST(_t as bigint) num_t"+
                ", from_unixtime(_t) reg_date_time"+
                ", CAST(date_format(from_unixtime(_t), 'EEEE') as CHAR(9)) _name_day " +
                ", next_day(from_unixtime(_t), 'Monday') _next_mon " +
                ", next_day(next_day(from_unixtime(_t), 'Monday'), 'Monday') _next_next_mon " +
                ", unix_timestamp(next_day(from_unixtime(_t), 'Monday')) _unix_next_mon"+
                ", from_unixtime(unix_timestamp(next_day(from_unixtime(_t), 'Monday'))) _from_unix_next_mon"+
                ", from_unixtime(7*24*60*60+unix_timestamp(next_day(from_unixtime(_t), 'Monday'))) _from_unix_next_next_mon"+
                ", CAST(date_format(next_day(from_unixtime(_t), 'Monday'), 'EEEE') as CHAR(9)) _day4 " +
                */
                "FROM events " +
                "WHERE _n='registered'");

    }
}