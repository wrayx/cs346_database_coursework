SELECT CONCAT("ss_store_sk_", ss_store_sk) AS store, SUM(ss_net_paid) AS net_paid 
FROM store_sales
WHERE ss_sold_date_sk > 2451146 
    AND ss_sold_date_sk < 2452268 
    AND ISNOTNULL(ss_store_sk) 
GROUP BY ss_store_sk
ORDER BY net_paid DESC LIMIT 10;

-- HIVE output:

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

-- MapReduce output:
-- ss_store_sk_8   479051954.38
-- ss_store_sk_7   479048569.13
-- ss_store_sk_2   477594514.73
-- ss_store_sk_10  476650853.89
-- ss_store_sk_1   475457349
-- ss_store_sk_4   475400665.43