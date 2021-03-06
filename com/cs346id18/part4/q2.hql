-- SELECT  CONCAT("ss_store_sk_", y.s_store_sk) AS store, 
--         y.s_floor_space AS floor_space,
--         coalesce(SUM(x.ss_net_paid), 0) AS net_paid 
-- FROM store_partitioned y LEFT JOIN store_sales_partitioned x
-- ON (y.s_store_sk = x.ss_store_sk)
-- WHERE ss_sold_date_sk >= 2451146 
--     AND ss_sold_date_sk <= 2452268 
--     AND ISNOTNULL(ss_store_sk) 
-- ORDER BY floor_space DESC, net_paid DESC LIMIT 10;

SELECT  CONCAT("ss_store_sk_", y.s_store_sk) AS ss_store_sk, 
        coalesce(net_paid, 0.00) AS net_paid, 
        s_floor_space AS floor_space
FROM 
    (SELECT ss_store_sk, SUM(ss_net_paid) AS net_paid 
    FROM store_sales_partitioned 
    WHERE ss_sold_date_sk >= 2451146 
    AND ss_sold_date_sk <= 2452268 
    AND ISNOTNULL(ss_store_sk)
    GROUP BY ss_store_sk) x
RIGHT JOIN store_partitioned y
ON x.ss_store_sk = y.s_store_sk
ORDER BY floor_space DESC, net_paid DESC LIMIT 10;

SELECT  CONCAT("ss_store_sk_", y.s_store_sk) AS ss_store_sk, 
        coalesce(net_paid, 0.00) AS net_paid, 
        s_floor_space AS floor_space
FROM 
    (SELECT ss_store_sk, SUM(ss_net_paid) AS net_paid 
    FROM store_sales
    WHERE ss_sold_date_sk >= 2451146 
    AND ss_sold_date_sk <= 2452268 
    AND ISNOTNULL(ss_store_sk)
    GROUP BY ss_store_sk) x
RIGHT JOIN store y
ON x.ss_store_sk = y.s_store_sk
ORDER BY floor_space DESC, net_paid DESC LIMIT 10;

-- partitioned

Total MapReduce CPU Time Spent: 2 minutes 41 seconds 300 msec
OK
+-----------------+----------------+--------------+
|   ss_store_sk   |    net_paid    | floor_space  |
+-----------------+----------------+--------------+
| ss_store_sk_34  | 2008009693.27  | 9810608      |
| ss_store_sk_27  | 0.00           | 9793063      |
| ss_store_sk_85  | 2009798937.92  | 9779755      |
| ss_store_sk_36  | 0.00           | 9604859      |
| ss_store_sk_94  | 2019263515.14  | 9599785      |
| ss_store_sk_46  | 2005095734.63  | 9589409      |
| ss_store_sk_48  | 0.00           | 9543300      |
| ss_store_sk_62  | 1994552456.40  | 9342076      |
| ss_store_sk_63  | 0.00           | 9342076      |
| ss_store_sk_4   | 2021699356.08  | 9341467      |
+-----------------+----------------+--------------+
10 rows selected (141.221 seconds)

-- normal table

-- Total MapReduce CPU Time Spent: 9 minutes 21 seconds 150 msec
-- OK
-- +-----------------+----------------+--------------+
-- |   ss_store_sk   |    net_paid    | floor_space  |
-- +-----------------+----------------+--------------+
-- | ss_store_sk_34  | 2008009693.27  | 9810608      |
-- | ss_store_sk_27  | 0.00           | 9793063      |
-- | ss_store_sk_85  | 2009798937.92  | 9779755      |
-- | ss_store_sk_36  | 0.00           | 9604859      |
-- | ss_store_sk_94  | 2019263515.14  | 9599785      |
-- | ss_store_sk_46  | 2005095734.63  | 9589409      |
-- | ss_store_sk_48  | 0.00           | 9543300      |
-- | ss_store_sk_62  | 1994552456.40  | 9342076      |
-- | ss_store_sk_63  | 0.00           | 9342076      |
-- | ss_store_sk_4   | 2021699356.08  | 9341467      |
-- +-----------------+----------------+--------------+
-- 10 rows selected (564.926 seconds)

-- Total MapReduce CPU Time Spent: 14 seconds 510 msec
-- OK
-- +-----------------+---------------+--------------+
-- |   ss_store_sk   |   net_paid    | floor_space  |
-- +-----------------+---------------+--------------+
-- | ss_store_sk_4   | 475400665.40  | 9341467      |
-- | ss_store_sk_10  | 476650853.94  | 9294113      |
-- | ss_store_sk_11  | 0.00          | 9294113      |
-- | ss_store_sk_5   | 0.00          | 9078805      |
-- | ss_store_sk_6   | 0.00          | 9026222      |
-- | ss_store_sk_7   | 479048569.12  | 8954883      |
-- | ss_store_sk_3   | 0.00          | 7557959      |
-- | ss_store_sk_8   | 479051954.37  | 6995995      |
-- | ss_store_sk_9   | 0.00          | 6995995      |
-- | ss_store_sk_2   | 477594514.78  | 5285950      |
-- +-----------------+---------------+--------------+
-- 10 rows selected (75.853 seconds)