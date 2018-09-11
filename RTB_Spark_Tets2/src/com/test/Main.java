package com.test;

import java.io.File;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        // write your code here
        System.out.println("Запущено приложение по анализу логов");
        Scanner userInput = new Scanner(System.in);

        String currFolder;
        String loadFile;
        if (args.length == 0) {
            //ветка для отладки
            currFolder = "c:\\RTB_Spark_Test\\";
            loadFile = "events.json";
            System.out.println("Приложение запущено в отладочном режиме!");
        }
        else {
            currFolder = args[0];
            loadFile = args[1];
        }

        File currFile = new File(currFolder+loadFile);
        if ((!currFile.exists())||(currFile.isDirectory())){
            System.out.println("Указанного файла "+ currFolder+loadFile + " не существует!");
            userInput.nextLine();
            return;
        }

        System.out.println("Устанавливается соединение со Spark...");
        RegLoadLogFile logFile = new RegLoadLogFile(currFolder, loadFile);
        logFile.init();
        System.out.println("Соединение со Spark успешно установлено. ID= " + logFile.getSpark().toString());

        String folderEvents = logFile.getWorkFolder() +
                logFile.getWorkFile().replace(".", "_") +
                "\\events\\";

        System.out.println("Идет выборка данных о зарегистрированных пользователях...");
        String fileRegistered = folderEvents + "registered";
        RegFilter currRegFilter = new RegFilter(folderEvents, "registered", logFile.getSpark());
        logFile.filterAndSave(currRegFilter);
        currRegFilter = null;//объект далее нам не нужен
        System.out.println("Данные о зарегистрированных пользователях успешно выбраны");

        System.out.println("Идет выборка данных о запусках приложения...");
        String fileAppLoaded = folderEvents+ "app_loaded";
        LoadFilter currLoadFilter = new LoadFilter(folderEvents, "app_loaded", logFile.getSpark());
        logFile.filterAndSave(currLoadFilter);
        currLoadFilter = null;//объект далее нам не нужен
        System.out.println("Данные о запусках приложения успешно выбраны");

        //System.out.println("Идет расчет процента активных пользователей...");
        //ActiveUserPrcnt CurrActiveUserPrcnt = new ActiveUserPrcnt(logFile.getSpark());
        //logFile.CalcStat(CurrActiveUserPrcnt);
        //System.out.println("Рассчитан процент(по json) активных пользователей: " + CurrActiveUserPrcnt.getResult().toString());
        //CurrActiveUserPrcnt = null;

        ActiveUserPrcntByParquet currActiveUserPrcntByParquet = new ActiveUserPrcntByParquet(fileRegistered, fileAppLoaded, logFile.getSpark());
        logFile.calcStat(currActiveUserPrcntByParquet);
        System.out.println("Рассчитан процент активных пользователей: " +
                String.format("%.2f", currActiveUserPrcntByParquet.getResult()));
        currActiveUserPrcntByParquet = null;//объект далее нам не нужен
        logFile = null;//объект далее нам не нужен
        System.out.println("Для выхода из программы нажмите любую клавишу...");
        userInput.nextLine();

    }

}





