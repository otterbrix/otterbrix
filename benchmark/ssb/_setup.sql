CREATE TABLE IF NOT EXISTS lineorder (lo_orderkey BIGINT, lo_linenumber BIGINT, lo_custkey BIGINT, lo_partkey BIGINT,
  lo_suppkey BIGINT, lo_orderdate BIGINT, lo_orderpriority TEXT, lo_shippriority BIGINT, lo_quantity BIGINT,
  lo_extendedprice BIGINT, lo_ordtotalprice BIGINT, lo_discount BIGINT, lo_revenue BIGINT, lo_supplycost BIGINT,
  lo_tax BIGINT, lo_commitdate BIGINT, lo_shipmode TEXT);
CREATE TABLE IF NOT EXISTS dim_date (d_datekey BIGINT, d_date TEXT, d_dayofweek TEXT, d_month TEXT, d_year BIGINT,
  d_yearmonthnum BIGINT, d_yearmonth TEXT, d_daynuminweek BIGINT, d_daynuminmonth BIGINT, d_daynuminyear BIGINT,
  d_monthnuminyear BIGINT, d_weeknuminyear BIGINT, d_sellingseason TEXT, d_lastdayinweekfl BIGINT,
  d_lastdayinmonthfl BIGINT, d_holidayfl BIGINT, d_weekdayfl BIGINT);
CREATE TABLE IF NOT EXISTS customer (c_custkey BIGINT, c_name TEXT, c_address TEXT, c_city TEXT, c_nation TEXT,
  c_region TEXT, c_phone TEXT, c_mktsegment TEXT);
CREATE TABLE IF NOT EXISTS part (p_partkey BIGINT, p_name TEXT, p_mfgr TEXT, p_category TEXT, p_brand1 TEXT,
  p_color TEXT, p_type TEXT, p_size BIGINT, p_container TEXT);
CREATE TABLE IF NOT EXISTS supplier (s_suppkey BIGINT, s_name TEXT, s_address TEXT, s_city TEXT, s_nation TEXT,
  s_region TEXT, s_phone TEXT);
-- @load_csv ../data/ssb/lineorder.tbl lineorder |
-- @load_csv ../data/ssb/date.tbl dim_date |
-- @load_csv ../data/ssb/customer.tbl customer |
-- @load_csv ../data/ssb/part.tbl part |
-- @load_csv ../data/ssb/supplier.tbl supplier |
