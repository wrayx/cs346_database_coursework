if [[ $1 == "rm" ]]
then
    hdfs dfs -rm -r output/q1b;
elif [[ $1 == "cat" ]]
then
    hdfs dfs -cat output/q1b/part-r-00000;
else
    rm com/cs346id18/com/cs346id18/part3/q1b/Top*.class
    rm TopKItems.jar
    hadoop com.sun.tools.javac.Main com/cs346id18/part3/q1b/TopKItems.java
    jar -cf TopKItems.jar com/cs346id18/part3/q1b/TopKItems*.class
    hadoop jar TopKItems.jar com.cs346id18.part3.q1b.TopKItems 5 2451146 2452268 input/1G/store_sales/store_sales.dat output/q1b
fi