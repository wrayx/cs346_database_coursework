SELECT CONCAT("ss_item_sk_", ss_item_sk) AS item, 
SUM(ss_quantity) AS quantity 
FROM store_sales
WHERE ss_sold_date_sk > 2451146 
    AND ss_sold_date_sk < 2452268 
    AND ISNOTNULL(ss_item_sk) 
GROUP BY ss_item_sk
ORDER BY quantity DESC LIMIT 10;

-- HIVE output:

-- Total MapReduce CPU Time Spent: 20 seconds 340 msec
-- OK
+-------------------+-----------+
|       item        | quantity  |
+-------------------+-----------+
| ss_item_sk_1279   | 12173     |
| ss_item_sk_5953   | 12077     |
| ss_item_sk_499    | 12075     |
| ss_item_sk_8533   | 12058     |
| ss_item_sk_7507   | 12032     |
| ss_item_sk_15229  | 11786     |
| ss_item_sk_5875   | 11778     |
| ss_item_sk_14059  | 11754     |
| ss_item_sk_16753  | 11705     |
| ss_item_sk_11383  | 11686     |
+-------------------+-----------+
-- 10 rows selected (42.21 seconds)

-- MapReduce output:
-- TBD