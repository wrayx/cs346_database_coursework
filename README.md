# CS346

## TODO
Part 3: 
- [ ] Query 1.a
- [ ] Query 1.b
- [ ] Query 1.c
- [ ] Query 2 (Reduce Side Join)

Part 4:
- [ ] Hive schema
- [ ] Query 1.a
- [ ] Query 1.b
- [ ] Query 1.c
- [ ] Query 2 (Reduce Side Join)

Report:
- [ ] ..


## Tutorials 
+ [MapReduce Program – Weather Data Analysis For Analyzing Hot And Cold Days
](https://www.geeksforgeeks.org/mapreduce-program-weather-data-analysis-for-analyzing-hot-and-cold-days/?ref=lbp)
+ [Apache Hive - Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)

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
```

## Hive commands
```bash

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