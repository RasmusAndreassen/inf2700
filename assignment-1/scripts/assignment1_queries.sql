-- write settings and queries here and run
-- sqlite3 inf2700_orders.sqlite3 < assignment1_queries.sql
-- to perform the queries
-- for example:

.mode column
.header on

SELECT DISTINCT productName, productVendor
FROM   Products
LIMIT  6;
