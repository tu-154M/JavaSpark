package com.test;

import java.io.File;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        // write your code here
        System.out.println("Запущено приложение по анализу логов");
        Scanner UserInput = new Scanner(System.in);

        String CurrFolder;
        String LoadFile;
        if (args.length == 0) {
            //ветка для отладки
            CurrFolder = "c:\\RTB_Spark_Test\\";
            LoadFile = "events.json";
            System.out.println("Приложение запущено в отладочном режиме!");
        }
        else {
            CurrFolder = args[0];
            LoadFile = args[1];
        }

        File CurrFile = new File(CurrFolder+LoadFile);
        if ((!CurrFile.exists())||(CurrFile.isDirectory())){
            System.out.println("Указанного файла "+ CurrFolder+LoadFile + " не существует!");
            UserInput.nextLine();
            return;
        }

        System.out.println("Устанавливается соединение со Spark...");
        TRegLoadLogFile LogFile = new TRegLoadLogFile(CurrFolder, LoadFile);
        LogFile.Init();
        System.out.println("Соединение со Spark успешно установлено. ID= " + LogFile.getSpark().toString());

        String FolderEvents = LogFile.getWorkFolder() +
                LogFile.getWorkFile().replace(".", "_") +
                "\\events\\";

        System.out.println("Идет выборка данных о зарегистрированных пользователях...");
        String FileRegistered = FolderEvents + "registered";
        TRegFilter RegFilter = new TRegFilter(FolderEvents, "registered", LogFile.getSpark());
        LogFile.Filter(RegFilter);
        RegFilter = null;//объект далее нам не нужен
        System.out.println("Данные о зарегистрированных пользователях успешно выбраны");

        System.out.println("Идет выборка данных о запусках приложения...");
        String FileAppLoaded = FolderEvents+ "app_loaded";
        TLoadFilter LoadFilter = new TLoadFilter(FolderEvents, "app_loaded", LogFile.getSpark());
        LogFile.Filter(LoadFilter);
        LoadFilter = null;//объект далее нам не нужен
        System.out.println("Данные о запусках приложения успешно выбраны");

        //System.out.println("Идет расчет процента активных пользователей...");
        //TActiveUserPrcnt ActiveUserPrcnt = new TActiveUserPrcnt(LogFile.getSpark());
        //LogFile.CalcStat(ActiveUserPrcnt);
        //System.out.println("Рассчитан процент(по json) активных пользователей: " + ActiveUserPrcnt.getResult().toString());

        TActiveUserPrcntByParquet ActiveUserPrcntByParquet = new TActiveUserPrcntByParquet(FileRegistered, FileAppLoaded, LogFile.getSpark());
        LogFile.CalcStat(ActiveUserPrcntByParquet);
        System.out.println("Рассчитан процент активных пользователей: " +
                String.format("%.2f", ActiveUserPrcntByParquet.getResult()));
        System.out.println("Для выхода из программы нажмите любую клавишу...");
        UserInput.nextLine();

    }

}





