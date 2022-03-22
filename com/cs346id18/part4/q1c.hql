SELECT CONCAT("ss_sold_date_sk", ss_sold_date_sk) AS sold_date, 
SUM(ss_net_paid_inc_tax) AS inc_tax 
FROM store_sales
WHERE ss_sold_date_sk >= 2451392 
    AND ss_sold_date_sk <= 2451894 
    AND ISNOTNULL(ss_sold_date_sk) 
GROUP BY ss_sold_date_sk
ORDER BY inc_tax DESC LIMIT 10;

SELECT CONCAT("ss_sold_date_sk", ss_sold_date_sk) AS sold_date, 
SUM(ss_net_paid_inc_tax) AS inc_tax 
FROM store_sales_partitioned
WHERE ss_sold_date_sk >= 2451392 
    AND ss_sold_date_sk <= 2451894 
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

-- partitioned table 

-- Total MapReduce CPU Time Spent: 2 minutes 38 seconds 110 msec
-- OK
-- +-------------------------+---------------+
-- |        sold_date        |    inc_tax    |
-- +-------------------------+---------------+
-- | ss_sold_date_sk2451546  | 265734057.05  |
-- | ss_sold_date_sk2451522  | 215615695.11  |
-- | ss_sold_date_sk2451544  | 214272014.63  |
-- | ss_sold_date_sk2451537  | 213133218.69  |
-- | ss_sold_date_sk2451521  | 212765624.69  |
-- | ss_sold_date_sk2451533  | 212126876.94  |
-- | ss_sold_date_sk2451532  | 211838600.76  |
-- | ss_sold_date_sk2451864  | 211408394.91  |
-- | ss_sold_date_sk2451851  | 211263811.59  |
-- | ss_sold_date_sk2451891  | 211243724.93  |
-- +-------------------------+---------------+
-- 10 rows selected (131.513 seconds)

-- normal table

-- Total MapReduce CPU Time Spent: 9 minutes 52 seconds 890 msec
-- OK
-- +-------------------------+---------------+
-- |        sold_date        |    inc_tax    |
-- +-------------------------+---------------+
-- | ss_sold_date_sk2451546  | 269011207.69  |
-- | ss_sold_date_sk2451522  | 218299535.32  |
-- | ss_sold_date_sk2451544  | 216896013.28  |
-- | ss_sold_date_sk2451537  | 215618328.78  |
-- | ss_sold_date_sk2451521  | 215392369.46  |
-- | ss_sold_date_sk2451533  | 214772795.73  |
-- | ss_sold_date_sk2451532  | 214618693.96  |
-- | ss_sold_date_sk2451851  | 213953384.07  |
-- | ss_sold_date_sk2451891  | 213918317.29  |
-- | ss_sold_date_sk2451880  | 213849908.61  |
-- +-------------------------+---------------+
-- 10 rows selected (467.232 seconds)

-- MapReduce output:
-- ss_sold_data_sk_2451546 7269283.12
-- ss_sold_data_sk_2451541 6176220.47
-- ss_sold_data_sk_2451853 6142654.77
-- ss_sold_data_sk_2451873 6011726.09
-- ss_sold_data_sk_2451488 5946318.8
-- ss_sold_data_sk_2451854 5859892.09
-- ss_sold_data_sk_2451868 5843080.66
-- ss_sold_data_sk_2451531 5794190.91
-- ss_sold_data_sk_2451537 5783052.86
-- ss_sold_data_sk_2451513 5761447.15