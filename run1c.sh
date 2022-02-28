if [[ $1 == "rm" ]]
then
    hdfs dfs -rm -r output/q1c;
elif [[ $1 == "cat" ]]
then
    hdfs dfs -cat output/q1c/part-r-00000;
else
    rm com/cs346id18/com/cs346id18/part3/q1c/Top*.class
    rm TopKDays.jar
    hadoop com.sun.tools.javac.Main com/cs346id18/part3/q1c/TopKDays.java
    jar -cf TopKDays.jar com/cs346id18/part3/q1c/TopKDays*.class
    hadoop jar TopKDays.jar com.cs346id18.part3.q1c.TopKDays 10 2451392 2451894 input/1G/store_sales/store_sales.dat output/q1c
fi