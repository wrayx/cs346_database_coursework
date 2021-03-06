SELECT CONCAT("ss_store_sk_", ss_store_sk) AS store, SUM(ss_net_paid) AS net_paid 
FROM store_sales_bucketed
WHERE ss_sold_date_sk >= 2451146 
    AND ss_sold_date_sk <= 2452268 
    AND ISNOTNULL(ss_store_sk) 
GROUP BY ss_store_sk
ORDER BY net_paid DESC LIMIT 10;

SELECT CONCAT("ss_store_sk_", ss_store_sk) AS store, SUM(ss_net_paid) AS net_paid 
FROM store_sales_partitioned
WHERE ss_sold_date_sk >= 2451146 
    AND ss_sold_date_sk <= 2452268 
    AND ISNOTNULL(ss_store_sk) 
GROUP BY ss_store_sk
ORDER BY net_paid DESC LIMIT 10;

-- HIVE output:

-- Total MapReduce CPU Time Spent: 18 seconds 50 msec
-- OK
-- +-----------------+---------------+
-- |      store      |   net_paid    |
-- +-----------------+---------------+
-- | ss_store_sk_8   | 479051954.37  |
-- | ss_store_sk_7   | 479048569.12  |
-- | ss_store_sk_2   | 477594514.78  |
-- | ss_store_sk_10  | 476650853.94  |
-- | ss_store_sk_1   | 475457349.02  |
-- | ss_store_sk_4   | 475400665.40  |
-- +-----------------+---------------+
-- 6 rows selected (42.936 seconds)

<<<<<<< HEAD
Total MapReduce CPU Time Spent: 2 minutes 40 seconds 730 msec
OK
+------------------+----------------+
|      store       |    net_paid    |
+------------------+----------------+
| ss_store_sk_92   | 2039983202.07  |
| ss_store_sk_50   | 2036624698.89  |
| ss_store_sk_8    | 2032733764.65  |
| ss_store_sk_32   | 2028710419.82  |
| ss_store_sk_4    | 2028145948.34  |
| ss_store_sk_94   | 2026137085.11  |
| ss_store_sk_38   | 2024183575.31  |
| ss_store_sk_82   | 2023916557.05  |
| ss_store_sk_97   | 2023659816.02  |
| ss_store_sk_112  | 2023012491.83  |
+------------------+----------------+
10 rows selected (111.243 seconds)
=======
-- 40G partitioned table results
-- Total MapReduce CPU Time Spent: 2 minutes 44 seconds 490 msec
-- OK
-- +-----------------+----------------+
-- |      store      |    net_paid    |
-- +-----------------+----------------+
-- | ss_store_sk_92  | 2033048405.98  |
-- | ss_store_sk_50  | 2029015206.80  |
-- | ss_store_sk_8   | 2025172472.22  |
-- | ss_store_sk_32  | 2022226982.26  |
-- | ss_store_sk_4   | 2021699356.08  |
-- | ss_store_sk_94  | 2019263515.14  |
-- | ss_store_sk_38  | 2017255687.69  |
-- | ss_store_sk_82  | 2016975812.03  |
-- | ss_store_sk_97  | 2016711251.01  |
-- | ss_store_sk_28  | 2015747152.24  |
-- +-----------------+----------------+
-- 10 rows selected (105.026 seconds)
>>>>>>> b2c4839f8a813ea4326f4e91c10b07f8e47a14bb

-- 40G partitioned and bucketed table results
-- Total MapReduce CPU Time Spent: 3 minutes 20 seconds 990 msec
-- OK
-- +-----------------+----------------+
-- |      store      |    net_paid    |
-- +-----------------+----------------+
-- | ss_store_sk_92  | 2033048405.98  |
-- | ss_store_sk_50  | 2029015206.80  |
-- | ss_store_sk_8   | 2025172472.22  |
-- | ss_store_sk_32  | 2022226982.26  |
-- | ss_store_sk_4   | 2021699356.08  |
-- | ss_store_sk_94  | 2019263515.14  |
-- | ss_store_sk_38  | 2017255687.69  |
-- | ss_store_sk_82  | 2016975812.03  |
-- | ss_store_sk_97  | 2016711251.01  |
-- | ss_store_sk_28  | 2015747152.24  |
-- +-----------------+----------------+
-- 10 rows selected (170.325 seconds)

-- MapReduce output:
-- ss_store_sk_8   479051954.38
-- ss_store_sk_7   479048569.13
-- ss_store_sk_2   477594514.73
-- ss_store_sk_10  476650853.89
-- ss_store_sk_1   475457349
-- ss_store_sk_4   475400665.43