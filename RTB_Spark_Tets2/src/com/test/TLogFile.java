package com.test;

public abstract class TLogFile {
    private String WorkFile;
    private String WorkFolder;

    public String getWorkFile(){
        return this.WorkFile;
    }

    public String getWorkFolder(){
        return WorkFolder;
    }

    public TLogFile(String inWorkFolder, String inWorkFile){
        WorkFolder = inWorkFolder;
        WorkFile = inWorkFile;
    }

    public abstract void Init();

    public void Filter(iFilter inFilter){
        inFilter.FilterAndSave();};

    public void CalcStat(iCalcStat inCalcStat){
        inCalcStat.CalcStat();
    };
}

 interface iFilter{
 void FilterAndSave();
}

interface iCalcStat{
    void CalcStat();
}