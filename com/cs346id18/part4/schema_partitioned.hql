CREATE EXTERNAL TABLE IF NOT EXISTS store(
    s_store_id CHAR(16), 
    s_rec_start_date DATE, 
    s_rec_end_date DATE, 
    s_closed_date_sk INT, 
    s_store_name VARCHAR(50), 
    s_number_employees INT, 
    s_floor_space INT, 
    s_hours CHAR(20), 
    S_manager VARCHAR(40), 
    S_market_id INT, 
    S_geography_class VARCHAR(100), 
    S_market_desc VARCHAR(100), 
    s_market_manager VARCHAR(40), 
    s_division_id INT, 
    s_division_name VARCHAR(50), 
    s_company_id INT, 
    s_company_name VARCHAR(50), 
    s_street_number VARCHAR(10), 
    s_street_name VARCHAR(60), 
    s_street_type CHAR(15), 
    s_suite_number CHAR(10), 
    s_city VARCHAR(60), 
    s_county VARCHAR(30), 
    s_state CHAR(2), 
    s_zip CHAR(10), 
    s_country VARCHAR(20), 
    s_gmt_offset DECIMAL(5,2), 
    s_tax_percentage DECIMAL(5,2), 
    PRIMARY KEY(s_store_sk) DISABLE)
PARTITIONED BY(s_store_sk INT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/user/cs346id18/input/1G/store/';


CREATE EXTERNAL TABLE IF NOT EXISTS store_sales(
    ss_sold_date_sk INT, 
    ss_sold_time_sk INT, 
    ss_item_sk INT, 
    ss_customer_sk INT, 
    ss_cdemo_sk INT, 
    ss_hdemo_sk INT, 
    ss_addr_sk INT, 
    ss_promo_sk INT, 
    ss_ticket_number INT, 
    ss_quantity INT, 
    ss_wholesale_cost DECIMAL(7,2), 
    ss_list_price DECIMAL(7,2), 
    ss_sales_price DECIMAL(7,2), 
    ss_ext_discount_amt DECIMAL(7,2), 
    ss_ext_sales_price DECIMAL(7,2), 
    ss_ext_wholesale_cost DECIMAL(7,2), 
    ss_ext_list_price DECIMAL(7,2), 
    ss_ext_tax DECIMAL(7,2), 
    ss_coupon_amt DECIMAL(7,2), 
    ss_net_paid DECIMAL(7,2), 
    ss_net_paid_inc_tax DECIMAL(7,2), 
    ss_net_profit DECIMAL(7,2),
    PRIMARY KEY(ss_item_sk, ss_ticket_number) DISABLE,
    CONSTRAINT c1 FOREIGN KEY (ss_store_sk) REFERENCES store(s_store_sk) DISABLE)
PARTITIONED BY(ss_store_sk INT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
LOCATION '/user/cs346id18/input/1G/store_sales';

