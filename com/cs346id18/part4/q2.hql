SELECT  CONCAT("ss_store_sk_", x.ss_store_sk) AS store, 
        s_floor_space AS floor_space,
        SUM(x.ss_net_paid) AS net_paid 
FROM store_partitioned y FULL OUTER JOIN store_sales_partitioned x
ON (x.ss_store_sk = y.s_store_sk)
WHERE ss_sold_date_sk > 2451146 
    AND ss_sold_date_sk < 2452268 
    AND ISNOTNULL(ss_store_sk) 
GROUP BY ss_store_sk, s_floor_space
ORDER BY floor_space DESC, net_paid DESC LIMIT 10;


-- Total MapReduce CPU Time Spent: 22 seconds 410 msec
-- OK
-- +-----------------+--------------+---------------+
-- |      store      | floor_space  |   net_paid    |
-- +-----------------+--------------+---------------+
-- | ss_store_sk_4   | 9341467      | 475400665.40  |
-- | ss_store_sk_10  | 9294113      | 476650853.94  |
-- | ss_store_sk_7   | 8954883      | 479048569.12  |
-- | ss_store_sk_8   | 6995995      | 479051954.37  |
-- | ss_store_sk_2   | 5285950      | 477594514.78  |
-- | ss_store_sk_1   | 5250760      | 475457349.02  |
-- +-----------------+--------------+---------------+
-- 6 rows selected (48.951 seconds)

-- Partitioned: 
-- Total MapReduce CPU Time Spent: 13 seconds 770 msec
-- OK
-- +-----------------+--------------+---------------+
-- |      store      | floor_space  |   net_paid    |
-- +-----------------+--------------+---------------+
-- | ss_store_sk_4   | 9341467      | 475400665.40  |
-- | ss_store_sk_10  | 9294113      | 476650853.94  |
-- | ss_store_sk_7   | 8954883      | 479048569.12  |
-- | ss_store_sk_8   | 6995995      | 479051954.37  |
-- | ss_store_sk_2   | 5285950      | 477594514.78  |
-- | ss_store_sk_1   | 5250760      | 475457349.02  |
-- +-----------------+--------------+---------------+
-- 6 rows selected (53.574 seconds)

