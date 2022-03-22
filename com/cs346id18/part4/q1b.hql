SELECT CONCAT("ss_item_sk_", ss_item_sk) AS item, 
SUM(ss_quantity) AS quantity 
FROM store_sales_bucketed
WHERE ss_sold_date_sk >= 2451146 
    AND ss_sold_date_sk <= 2452268 
    AND ISNOTNULL(ss_item_sk) 
GROUP BY ss_item_sk
ORDER BY quantity DESC LIMIT 10;

SELECT CONCAT("ss_item_sk_", ss_item_sk) AS item, 
SUM(ss_quantity) AS quantity 
FROM store_sales_partitioned
WHERE ss_sold_date_sk >= 2451146 
    AND ss_sold_date_sk <= 2452268 
    AND ISNOTNULL(ss_item_sk) 
GROUP BY ss_item_sk
ORDER BY quantity DESC LIMIT 10;

-- HIVE output:

-- Total MapReduce CPU Time Spent: 22 seconds 310 msec
-- OK
-- +------------------+-----------+
-- |       item       | quantity  |
-- +------------------+-----------+
-- | ss_item_sk_1279  | 12173     |
-- | ss_item_sk_5953  | 12077     |
-- | ss_item_sk_499   | 12075     |
-- | ss_item_sk_8533  | 12058     |
-- | ss_item_sk_7507  | 12032     |
-- +------------------+-----------+
-- 5 rows selected (48.815 seconds)

-- partitioned table 

-- Total MapReduce CPU Time Spent: 3 minutes 7 seconds 650 msec
-- OK
-- +-------------------+-----------+
-- |       item        | quantity  |
-- +-------------------+-----------+
-- | ss_item_sk_42163  | 141287    |
-- | ss_item_sk_42415  | 140664    |
-- | ss_item_sk_6031   | 140125    |
-- | ss_item_sk_37267  | 139776    |
-- | ss_item_sk_67     | 139345    |
-- | ss_item_sk_15139  | 139229    |
-- | ss_item_sk_3337   | 139170    |
-- | ss_item_sk_21883  | 139038    |
-- | ss_item_sk_33655  | 138886    |
-- | ss_item_sk_4651   | 138880    |
-- +-------------------+-----------+
-- 10 rows selected (135.551 seconds

--bucketed table

-- Total MapReduce CPU Time Spent: 3 minutes 38 seconds 490 msec
-- OK
-- +-------------------+-----------+
-- |       item        | quantity  |
-- +-------------------+-----------+
-- | ss_item_sk_42163  | 141287    |
-- | ss_item_sk_42415  | 140664    |
-- | ss_item_sk_6031   | 140125    |
-- | ss_item_sk_37267  | 139776    |
-- | ss_item_sk_67     | 139345    |
-- | ss_item_sk_15139  | 139229    |
-- | ss_item_sk_3337   | 139170    |
-- | ss_item_sk_21883  | 139038    |
-- | ss_item_sk_33655  | 138886    |
-- | ss_item_sk_4651   | 138880    |
-- +-------------------+-----------+
-- 10 rows selected (140.019 seconds)

-- MapReduce output:
-- ss_item_sk_1279 12173
-- ss_item_sk_5953 12077
-- ss_item_sk_499  12075
-- ss_item_sk_8533 12058
-- ss_item_sk_7507 12032