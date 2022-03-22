SELECT CONCAT("ss_item_sk_", ss_item_sk) AS item, 
SUM(ss_quantity) AS quantity 
FROM store_sales_bucketed
WHERE ss_sold_date_sk > 2451146 
    AND ss_sold_date_sk < 2452268 
    AND ISNOTNULL(ss_item_sk) 
GROUP BY ss_item_sk
ORDER BY quantity DESC LIMIT 5;

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

-- MapReduce output:
-- ss_item_sk_1279 12173
-- ss_item_sk_5953 12077
-- ss_item_sk_499  12075
-- ss_item_sk_8533 12058
-- ss_item_sk_7507 12032