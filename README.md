# CS346

## TODOs
Part 3 Java Map-Reduce: 
- [x] Query 1.a
- [x] Query 1.b
- [x] Query 1.c
- [ ] Query 2 (Reduce Side Join)

Part 4 Hive Query:
- [x] Hive schema
- [x] Query 1.a
- [x] Query 1.b
- [x] Query 1.c
- [ ] Query 2 (Reduce Side Join)

Additionally:
- [ ] Comment software code
- [ ] EDA for csv files
- [ ] Performance analyses

Report:
- [ ] ..


## Tutorials 
+ [MapReduce Program – Weather Data Analysis For Analyzing Hot And Cold Days
](https://www.geeksforgeeks.org/mapreduce-program-weather-data-analysis-for-analyzing-hot-and-cold-days/?ref=lbp)
+ [Apache Hive - Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)
+ [Importing Data from Files into Hive Tables](https://www.informit.com/articles/article.aspx?p=2756471&seqNum=4)

## Git
[Git cheat sheet](https://education.github.com/git-cheat-sheet-education.pdf)

## Hadoop Commands
```bash
# every time you log in, you must run the following in order to set some important environment variables:
source cs346env.sh
# no longer required, command has been added to .bashrc

# start the hadoop service
$HADOOP_HOME/sbin/start-all.sh

# stop all services
$HADOOP_HOME/sbin/stop-all.sh

# check that all daemons are up and running
jps

# turning off safe mode
hdfs dfsadmin -safemode leave

# running query 1 java map reduce 
bin/hadoop jar your_package.jar main K start_date end_date input_file output_directory

# hdfs listing files
hdfs dfs -ls input/1G/store

# hdfs cat dat file first line
hdfs dfs -cat input/1G/store/store.dat>&1 | head -n 1

# Compile java code
hadoop com.sun.tools.javac.Main WordCount.java

# Create jar file 
jar cf wc.jar WordCount*.class

# run program
hadoop jar wc.jar WordCount input/wc output/wclabsheet

# view results
hdfs dfs -ls output/wclabsheet
hdfs dfs -cat output/wclabsheet/part-r-00000

# delete output
hdfs dfs -rm -r output/*

# query 2 mapreduce output 
ss_store_sk_4   475400665.40    9341467
ss_store_sk_10  476650853.94    9294113
ss_store_sk_11  000000000.00    9294113
ss_store_sk_5   000000000.00    9078805
ss_store_sk_6   000000000.00    9026222
ss_store_sk_7   479048569.12    8954883
ss_store_sk_3   000000000.00    7557959
ss_store_sk_8   479051954.37    6995995
ss_store_sk_9   000000000.00    6995995
ss_store_sk_2   477594514.78    5285950
ss_store_sk_1   475457349.02    5250760
ss_store_sk_12  000000000.00    5219562


```

## Hive commands
```bash

Try setting those properties to higher values.

SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

# start Beeline client
$HIVE_HOME/bin/beeline -u jdbc:hive2://

# lab3 sql command 
SELECT colour, MAX(height*width) AS area FROM rectangles10m GROUP BY colour;
# to obtain:
+---------+-----------+
| colour  |   area    |
+---------+-----------+
| blue    | 99920007  |
| green   | 99910008  |
| yellow  | 99870012  |
| red     | 99820077  |
+---------+-----------+
```

