package com.test;

public abstract class LogFile {
    private String workFile;
    private String workFolder;

    public String getWorkFile(){
        return this.workFile;
    }

    public String getWorkFolder(){
        return workFolder;
    }

    public LogFile(String inWorkFolder, String inWorkFile){
        workFolder = inWorkFolder;
        workFile = inWorkFile;
    }

    public abstract void init();

    public void filterAndSave(Filter inFilter){
        inFilter.filterAndSave();
    };

    public void calcStat(CalcStat inCalcStat){
        inCalcStat.calcStat();
    };
}

 interface Filter{
 void filterAndSave();
}

interface CalcStat{
    void calcStat();
}