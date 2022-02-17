rm Top*.class
rm TopK.jar
hadoop com.sun.tools.javac.Main TopKNetProfit.java
jar cf TopK.jar TopKNetProfit*.class
hadoop jar TopK.jar TopKNetProfit 10 2451146 2452268 input/1G/store_sales/store_sales.dat output/q1
