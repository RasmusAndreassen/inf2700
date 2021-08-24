-- queries_messages.sql
-- run sqlite3 messages.sqlite3 < queries_messages.sql

.mode list
.headers on

-- all_countries
SELECT Distinct country
FROM   users;

-- messages_on_2018_03_01
SELECT content
FROM   messages
WHERE  date = '2018-03-01';

-- messages_to_people_other_than_the_author
SELECT DISTINCT date, content
FROM   messages NATURAL JOIN recipients
WHERE  uid != author;

-- messages_only_to_the_original_author
SELECT DISTINCT date, content
FROM   messages NATURAL JOIN recipients
WHERE  mid NOT IN
       (SELECT mid
        FROM messages NATURAL JOIN recipients
        WHERE author != uid);

-- poeple_who_did_not_send_any_message_on_2018_03_01
SELECT DISTINCT name, country
FROM   users
WHERE  uid NOT IN
       (SELECT author
        FROM messages
        WHERE date = '2018-03-01');

-- number_of_different_countries
SELECT COUNT(DISTINCT country) AS number_of_countries
FROM   users;

-- contents_authors_and_dates_addressed_to_Eva_in_Norway_in_the_order_of_dates
SELECT content, a.name, date
FROM   messages m NATURAL JOIN recipients r, users e, users a
WHERE  e.name = 'Eva' AND e.country = "Norway" AND  e.uid = r.uid
       AND r.read = 0 AND m.author = a.uid
ORDER BY date;

-- messages_with_number_of_recipients
SELECT mid, COUNT(*) AS number_of_recipients
FROM   recipients
GROUP BY mid
ORDER BY number_of_recipients DESC;

-- messages_with_at_least_3_recipients_on_2018_03_01
SELECT mid, COUNT(*) AS number_of_recipients
FROM   messages NATURAL JOIN recipients
WHERE  date = '2018-03-01'
GROUP BY mid
HAVING number_of_recipients >= 3;

-- people_who_sent_a_message_to_everybody_except_himself
SELECT  a.name, a.country
FROM    users a JOIN messages m ON author = uid
WHERE  NOT EXISTS
(SELECT uid    -- everybody
 FROM   users
 WHERE  uid != a.uid
 EXCEPT
 SELECT uid    -- recipients of m
 FROM   recipients
 WHERE  mid = m.mid);

-- empty_result
SELECT country
FROM   users
WHERE  name = "non existent";

-- erroneous_query
SELECT x
FROM   fake_tbl;

-- wrong_result
SELECT "wrong result";
