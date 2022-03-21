CREATE TABLE IF NOT EXISTS store_partitioned(
    s_floor_space INT, 
    PRIMARY KEY(s_store_sk) DISABLE)
PARTITIONED BY(s_store_sk INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

INSERT OVERWRITE TABLE store_partitioned partition(s_store_sk) SELECT s_floor_space, s_store_sk FROM store;

CREATE TABLE IF NOT EXISTS store_sales_partitioned(
    ss_sold_date_sk INT, 
    ss_net_paid DECIMAL(7,2),
    CONSTRAINT constraint1 FOREIGN KEY (ss_store_sk) REFERENCES store_partitioned(s_store_sk) DISABLE)
PARTITIONED BY(ss_store_sk INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

INSERT OVERWRITE TABLE store_sales_partitioned partition(ss_store_sk) SELECT ss_sold_date_sk, ss_net_paid, ss_store_sk FROM store_sales;
