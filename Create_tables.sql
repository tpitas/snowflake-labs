-- Create Database mydata_db
CREATE DATABASE IF NOT EXISTS mydata_db;
USE DATABASE mydata_db;

-- Create table region
CREATE TABLE region 
AS SELECT * FROM snowflake_sample_data.tpch_sf1.region;

-- Create table nation
CREATE TABLE nation 
AS SELECT * FROM snowflake_sample_data.tpch_sf1.nation;

-- Create table part
CREATE TABLE part AS
SELECT * FROM snowflake_sample_data.tpch_sf1.part 
WHERE MOD(p_partkey,50) = 8;

-- Create table partsupp
CREATE TABLE partsupp AS
SELECT * FROM snowflake_sample_data.tpch_sf1.partsupp
WHERE MOD(ps_partkey,50) = 8;

-- Create table supplier
CREATE TABLE supplier AS
WITH sp AS (SELECT DISTINCT ps_suppkey FROM partsupp)
SELECT su.* FROM snowflake_sample_data.tpch_sf1.supplier su
INNER JOIN sp
ON su.s_suppkey = sp.ps_suppkey;

-- Create table lineitem
CREATE TABLE lineitem AS
SELECT li.* FROM snowflake_sample_data.tpch_sf1.lineitem li
INNER JOIN part pa
ON pa.p_partkey = li.l_partkey;

-- Create table orders
CREATE TABLE orders AS
WITH lit AS (SELECT DISTINCT l_orderkey FROM lineitem)
SELECT ord.* FROM snowflake_sample_data.tpch_sf1.orders ord
INNER JOIN lit ON ord.o_orderkey = lit.l_orderkey;

-- Create table customer
CREATE TABLE customer AS
WITH ord AS (SELECT DISTINCT o_custkey FROM orders)
SELECT cu.* FROM snowflake_sample_data.tpch_sf1.customer cu
INNER JOIN ord ON cu.c_custkey = ord.o_custkey;

-- Dealing with joins
SELECT ord.o_orderkey, ord.o_orderstatus, ord.o_orderdate, 
cu.c_name
FROM orders ord 
INNER JOIN customer cu
ON ord.o_custkey = cu.c_custkey
LIMIT 5;

SELECT n_name AS nation_name, COUNT(*) AS number_of_suppliers
FROM supplier su
JOIN nation na
ON su.s_nationkey = na.n_nationkey
GROUP BY n_name
ORDER BY n_name
;

-- Dealing with sets
-- Union
SELECT DISTINCT o_custkey
FROM orders
WHERE o_totalprice > 400000
AND DATE_PART(year, o_orderdate) = 1995
UNION
SELECT DISTINCT o_custkey
FROM orders
WHERE o_totalprice > 400000
AND DATE_PART(year, o_orderdate) = 1997;

-- Intersect
SELECT DISTINCT o_custkey
FROM orders
WHERE o_totalprice > 350000
AND DATE_PART(year, o_orderdate) = 1995
INTERSECT
SELECT DISTINCT o_custkey
FROM orders
WHERE o_totalprice > 350000
AND DATE_PART(year, o_orderdate) = 1997;

-- Except
SELECT DISTINCT o_custkey
FROM orders
WHERE o_totalprice > 400000
AND DATE_PART(year, o_orderdate) = 1995
EXCEPT
SELECT DISTINCT o_custkey
FROM orders
WHERE o_totalprice > 400000
AND DATE_PART(year, o_orderdate) = 1997;

-- Sorting Compound Query Results
SELECT DISTINCT o_orderdate FROM orders
INTERSECT
SELECT DISTINCT l_shipdate FROM lineitem
ORDER BY o_orderdate;