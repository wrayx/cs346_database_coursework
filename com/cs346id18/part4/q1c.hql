SELECT CONCAT("ss_sold_date_sk", ss_sold_date_sk) AS sold_date, 
SUM(ss_net_paid_inc_tax) AS inc_tax 
FROM store_sales
WHERE ss_sold_date_sk > 2451392 
    AND ss_sold_date_sk < 2451894 
    AND ISNOTNULL(ss_sold_date_sk) 
GROUP BY ss_sold_date_sk
ORDER BY inc_tax DESC LIMIT 10;

-- HIVE output:

-- Total MapReduce CPU Time Spent: 20 seconds 30 msec
-- OK
-- +-------------------------+-------------+
-- |        sold_date        |   inc_tax   |
-- +-------------------------+-------------+
-- | ss_sold_date_sk2451546  | 7269283.12  |
-- | ss_sold_date_sk2451541  | 6176220.47  |
-- | ss_sold_date_sk2451853  | 6142654.77  |
-- | ss_sold_date_sk2451873  | 6011726.09  |
-- | ss_sold_date_sk2451488  | 5946318.80  |
-- | ss_sold_date_sk2451854  | 5859892.09  |
-- | ss_sold_date_sk2451868  | 5843080.66  |
-- | ss_sold_date_sk2451531  | 5794190.91  |
-- | ss_sold_date_sk2451537  | 5783052.85  |
-- | ss_sold_date_sk2451513  | 5761447.16  |
-- +-------------------------+-------------+
-- 10 rows selected (44.086 seconds)

-- MapReduce output:
-- TBD