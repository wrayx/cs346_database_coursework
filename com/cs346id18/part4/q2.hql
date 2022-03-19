SELECT  CONCAT("ss_store_sk_", x.ss_store_sk) AS store, 
        SUM(y.s_floor_space ) AS floor_space,
        SUM(x.ss_net_paid) AS net_paid 
FROM store_sales x FULL OUTER JOIN store y 
ON (x.ss_store_sk = y.s_store_sk)
WHERE ss_sold_date_sk > 2451146 
    AND ss_sold_date_sk < 2452268 
    AND ISNOTNULL(ss_store_sk) 
GROUP BY ss_store_sk
ORDER BY floor_space DESC, net_paid DESC LIMIT 10;


Total MapReduce CPU Time Spent: 22 seconds 620 msec
OK
+-----------------+----------------+---------------+
|      store      |  floor_space   |   net_paid    |
+-----------------+----------------+---------------+
| ss_store_sk_10  | 2610325988954  | 476650853.94  |
| ss_store_sk_4   | 2609389342978  | 475400665.40  |
| ss_store_sk_7   | 2520369730116  | 479048569.12  |
| ss_store_sk_8   | 1961697985985  | 479051954.37  |
| ss_store_sk_2   | 1472216364250  | 477594514.78  |
| ss_store_sk_1   | 1466090953400  | 475457349.02  |
+-----------------+----------------+---------------+
6 rows selected (56.273 seconds)

