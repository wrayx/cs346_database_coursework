SELECT CONCAT("ss_store_sk_", ss_store_sk) AS store, SUM(ss_net_paid) AS net_paid 
FROM store_sales
WHERE ss_sold_date_sk > 2451146 
    AND ss_sold_date_sk < 2452268 
    AND ISNOTNULL(ss_store_sk) 
GROUP BY ss_store_sk
ORDER BY net_paid DESC LIMIT 5;

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

-- MapReduce output:
-- ss_store_sk_8   479051954.38
-- ss_store_sk_7   479048569.13
-- ss_store_sk_2   477594514.73
-- ss_store_sk_10  476650853.89
-- ss_store_sk_1   475457349
-- ss_store_sk_4   475400665.43