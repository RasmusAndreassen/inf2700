-- write settings and queries here and run
-- sqlite3 inf2700_orders.sqlite3 < assignment1_queries.sql
-- to perform the queries
-- for example:

.mode column
.header on


SELECT C.customerName AS Customer, SUM(OD.quantityOrdered) AS Total
FROM   Orders O, Customers C, OrderDetails OD
WHERE  O.customerNumber = C.customerNumber
    AND O.orderNumber = OD.orderNumber
GROUP BY O.customerNumber
ORDER BY Total DESC;