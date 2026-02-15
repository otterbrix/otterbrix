SELECT 
  d_year,
  s_city,
  p_brand1,
  SUM(lo_revenue - lo_supplycost) AS profit
FROM 
  dim_date,
  customer,
  supplier,
  part,
  lineorder
WHERE 
  lo_custkey = c_custkey
  AND lo_suppkey = s_suppkey
  AND lo_partkey = p_partkey
  AND lo_orderdate = d_datekey
  AND c_region = 'AMERICA'
  AND s_nation = 'UNITED STATES'
  AND (d_year = 1997
    OR d_year = 1998)
  AND p_category = 'MFGR#14'
GROUP BY 
  d_year,
  s_city,
  p_brand1
ORDER BY 
  d_year,
  s_city,
  p_brand1;
