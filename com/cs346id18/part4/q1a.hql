SELECT ss_store_sk, SUM(ss_net_paid) AS net_paid FROM store_sales
WHERE ss_sold_date_sk > 2451146 
    AND ss_sold_date_sk < 2452268 
    AND isnotnull(ss_store_sk) 
GROUP BY ss_store_sk
ORDER BY net_paid DESC;

-- HIVE output:

-- Total MapReduce CPU Time Spent: 16 seconds 200 msec
-- OK
-- +--------------+---------------+
-- | ss_store_sk  |   net_paid    |
-- +--------------+---------------+
-- | 8            | 479051954.37  |
-- | 7            | 479048569.12  |
-- | 2            | 477594514.78  |
-- | 10           | 476650853.94  |
-- | 1            | 475457349.02  |
-- | 4            | 475400665.40  |
-- +--------------+---------------+
-- 6 rows selected (42.337 seconds)

-- MapReduce output:
-- ss_store_sk_8   479.05
-- ss_store_sk_7   479.05
-- ss_store_sk_2   477.59
-- ss_store_sk_10  476.65
-- ss_store_sk_1   475.46
-- ss_store_sk_4   475.4