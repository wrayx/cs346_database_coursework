SELECT CONCAT("ss_sold_date_sk", ss_sold_date_sk) AS sold_date, 
SUM(ss_net_paid_inc_tax) AS inc_tax 
FROM store_sales
WHERE ss_sold_date_sk > 2451146 
    AND ss_sold_date_sk < 2452268 
    AND ISNOTNULL(ss_sold_date_sk) 
GROUP BY ss_sold_date_sk
ORDER BY inc_tax DESC LIMIT 10;

-- HIVE output:

--Total MapReduce CPU Time Spent: 18 seconds 990 msec
--OK
+-------------------------+-------------+
|        sold_date        |   inc_tax   |
+-------------------------+-------------+
| ss_sold_date_sk2451546  | 7269283.12  |
| ss_sold_date_sk2451181  | 7093748.90  |
| ss_sold_date_sk2451167  | 6198097.06  |
| ss_sold_date_sk2451541  | 6176220.47  |
| ss_sold_date_sk2451853  | 6142654.77  |
| ss_sold_date_sk2451873  | 6011726.09  |
| ss_sold_date_sk2452223  | 6000666.26  |
| ss_sold_date_sk2451488  | 5946318.80  |
| ss_sold_date_sk2451160  | 5928366.98  |
| ss_sold_date_sk2452262  | 5904177.31  |
+-------------------------+-------------+
--10 rows selected (42.052 seconds)

-- MapReduce output:
-- TBD