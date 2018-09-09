Описание: 
Java-программа анализирует исходный json-файл при помощи Apache Spark, используя SQL-запросы. 
Исходный json-файл фильтруется SQL-запросами и разбивается на perquet-файлы. 
На parquet-файлах выполняются SQL-запросы для расчета аналитического показателя, который выводится на экран.


Используемое ПО:
1. ОС Windows 7 домашнаяя базовая
2. java version "1.8.0_144"
   Java(TM) SE Runtime Environment (build 1.8.0_144-b01)
   Java HotSpot(TM) 64-Bit Server VM (build 25.144-b01, mixed mode)
3. Scala code runner version 2.11.8 -- Copyright 2002-2016, LAMP/EPFL
4. Spark version 2.3.1 (standalone)
   Spark context available as 'sc' (master = local[*], app id = local-1536350699117).
   Spark session available as 'spark'.
5. IDE для разработки IntelliJ IDEA 2018.2.3 Community Edition


Порядок запуска:
1. Для запуска программы под отладкой необходимо наличие папки c:\RTB_Spark_Test\ 
и json-файла в ней с данными для анализа с именем events.json

2. В результате работы программы в этой папке будут созданы подпапки с отфильтрованными данными
в виде паркет-файлов:
c:\RTB_Spark_Test\events_json\events\app_loaded\
и
c:\RTB_Spark_Test\events_json\events\registered\

Аналитическая информация из этих файлов будет выведена в консоль.
