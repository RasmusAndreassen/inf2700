-- write settings and queries here and run
-- sqlite3 inf2700_orders.sqlite3 < assignment1_queries.sql
-- to perform the queries
-- for example:

.mode column
.header on

--1------------------------------------------------------------------------------------------------------------------------

SELECT  customerName
FROM    Customers
WHERE   UPPER(TRIM(country)) IS 'NORWAY';

--2-------------------------------------------------------------------------------------------------------------------------

SELECT productName, productScale
FROM Products
WHERE productLine IS 'Classic Cars';

--3-------------------------------------------------------------------------------------------------------------------------

SELECT  O.orderNumber, O.requiredDate, P.productName, OD.quantityOrdered, P.quantityInStock
FROM    OrderDetails OD, Orders O, Products P
WHERE   OD.orderNumber = O.orderNumber
  AND   OD.productCode = P.productCode
  AND   O.status = 'In Process';

--4-------------------------------------------------------------------------------------------------------------------------

SELECT  C.customerName, C.creditLimit, Pr.totalPrice, Pm.totalPayment, Pr.totalPrice-Pm.totalPayment AS diffPricePayment
FROM    Customers C NATURAL JOIN
        ( SELECT  customerNumber, SUM(amount) AS totalPayment
          FROM    Payments
          GROUP BY customerNumber) AS Pm,
        ( SELECT O.customerNumber, SUM(OD.quantityOrdered*OD.priceEach) AS totalPrice
          FROM Orders O, OrderDetails OD
          WHERE O.orderNumber = OD.orderNumber
          GROUP BY O.customerNumber) AS Pr
WHERE   C.customerNumber = Pr.customerNumber
  AND   C.customernumber = Pm.customerNumber
  AND   diffPricePayment > C.creditLimit;

--5-------------------------------------------------------------------------------------------------------------------------

SELECT  customerNumber, customerName
FROM  (SELECT DISTINCT customerNumber, customerName, productCode
       FROM   Customers C NATURAL JOIN Orders O NATURAL JOIN OrderDetails OD NATURAL JOIN
              (SELECT  sOD.productCode
               FROM    Orders sO NATURAL JOIN OrderDetails sOD
               WHERE   sO.customerNumber = 219) KL/* key list*/) L --actual list
GROUP BY  customerNumber
HAVING  COUNT(*) = ALL (SELECT COUNT(*)
        FROM    Orders sO NATURAL JOIN OrderDetails sOD
        WHERE   sO.customerNumber = 219)
   AND customerNumber != 219
ORDER BY customerNumber;
