rm com/cs346id18/com/cs346id18/part3/q1a/Top*.class
rm TopKNetProfit.jar
hadoop com.sun.tools.javac.Main com/cs346id18/part3/q1a/TopKNetProfit.java
jar -cf TopKNetProfit.jar com/cs346id18/part3/q1a/TopKNetProfit*.class
hadoop jar TopKNetProfit.jar com.cs346id18.part3.q1a.TopKNetProfit 10 2451146 2452268 input/1G/store_sales/store_sales.dat output/q1
