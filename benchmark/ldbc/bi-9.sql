/* Q9. Top thread initiators
\set startDate '\'2011-10-01\''::timestamp
\set endDate '\'2011-10-15\''::timestamp
 */
WITH
MPP AS (SELECT RootPostId, count(*) as MessageCount FROM Message WHERE Message.creationDate BETWEEN :startDate AND :endDate GROUP BY RootPostId)
SELECT Person.id AS "person.id"
     , Person.firstName AS "person.firstName"
     , Person.lastName AS "person.lastName"
     , count(Post.id) AS threadCount
     , sum(MPP.MessageCount) AS messageCount
  FROM Person
  JOIN Post_View Post
    ON Person.id = Post.CreatorPersonId
  JOIN MPP
    ON Post.id = MPP.RootPostId
 WHERE Post.creationDate BETWEEN :startDate AND :endDate
 GROUP BY Person.id, Person.firstName, Person.lastName
 ORDER BY messageCount DESC, Person.id
 LIMIT 100
;
