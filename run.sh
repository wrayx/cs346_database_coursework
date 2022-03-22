#!/bin/sh
if [[ $1 == "a" ]]; then
    if [[ $2 == "rm" ]]; then
        hdfs dfs -rm -r output/q1a
    elif [[ $2 == "cat" ]]; then
        hdfs dfs -cat output/q1a/part-r-00000
    else
        rm com/cs346id18/com/cs346id18/part3/q1a/Top*.class
        rm TopKNetProfit.jar
        hadoop com.sun.tools.javac.Main com/cs346id18/part3/q1a/TopKNetProfit.java
        jar -cf TopKNetProfit.jar com/cs346id18/part3/q1a/TopKNetProfit*.class
        hadoop jar TopKNetProfit.jar com.cs346id18.part3.q1a.TopKNetProfit 10 2451146 2452268 input/40G/store_sales/store_sales.dat output/q1a
    fi
elif [[ $1 == "b" ]]; then
    if [[ $2 == "rm" ]]; then
        hdfs dfs -rm -r output/q1b
    elif [[ $2 == "cat" ]]; then
        hdfs dfs -cat output/q1b/part-r-00000
    else
        rm com/cs346id18/com/cs346id18/part3/q1b/Top*.class
        rm TopKItems.jar
        hadoop com.sun.tools.javac.Main com/cs346id18/part3/q1b/TopKItems.java
        jar -cf TopKItems.jar com/cs346id18/part3/q1b/TopKItems*.class
        hadoop jar TopKItems.jar com.cs346id18.part3.q1b.TopKItems 5 2451146 2452268 input/40G/store_sales/store_sales.dat output/q1b
    fi
elif [[ $1 == "c" ]]; then
    if [[ $2 == "rm" ]]; then
        hdfs dfs -rm -r output/q1c
    elif [[ $2 == "cat" ]]; then
        hdfs dfs -cat output/q1c/part-r-00000
    else
        rm com/cs346id18/com/cs346id18/part3/q1c/Top*.class
        rm TopKDays.jar
        hadoop com.sun.tools.javac.Main com/cs346id18/part3/q1c/TopKDays.java
        jar -cf TopKDays.jar com/cs346id18/part3/q1c/TopKDays*.class
        hadoop jar TopKDays.jar com.cs346id18.part3.q1c.TopKDays 10 2451392 2451894 input/40G/store_sales/store_sales.dat output/q1c
    fi
elif [[ $1 == "2" ]]; then
    if [[ $2 == "rm" ]]; then
        hdfs dfs -rm -r output/q2
    elif [[ $2 == "cat" ]]; then
        hdfs dfs -cat output/q2/part-r-00000
    else
        rm com/cs346id18/com/cs346id18/part3/q2/Top*.class
        rm TopKFloorSpace.jar
        hadoop com.sun.tools.javac.Main com/cs346id18/part3/q2/TopKFloorSpace.java
        jar -cf TopKFloorSpace.jar com/cs346id18/part3/q2/TopKFloorSpace*.class
        hadoop jar TopKFloorSpace.jar com.cs346id18.part3.q2.TopKFloorSpace 10 2451146 2452268 input/1G/store_sales/store_sales.dat input/1G/store/store.dat output/q2
    fi
else
    echo "usage: ./run.sh <query: a/b/c> <action: rm/cat/compile>"
fi
