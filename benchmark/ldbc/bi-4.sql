/* Q4. Top message creators by country
\set date '\'2012-09-01\''::timestamp
 */
WITH Top100_Popular_Forums AS (
  SELECT id, creationDate, maxNumberOfMembers
  FROM Top100PopularForumsQ04
  WHERE creationDate > :date
  ORDER BY maxNumberOfMembers DESC, id
  LIMIT 100
)
SELECT au.id AS "person.id"
     , au.firstName AS "person.firstName"
     , au.lastName AS "person.lastName"
     , au.creationDate
     -- a single person might be member of more than 1 of the top100 forums, so their messages should be DISTINCT counted
     , count(Message.MessageId) AS messageCount
  FROM
       Person au
       LEFT JOIN Message
              ON au.id = Message.CreatorPersonId
             AND Message.ContainerForumId IN (SELECT id FROM Top100_Popular_Forums)
  WHERE EXISTS (SELECT 1
                FROM Top100_Popular_Forums
                INNER JOIN Forum_hasMember_Person
                        ON Forum_hasMember_Person.ForumId = Top100_Popular_Forums.id
                WHERE Forum_hasMember_Person.PersonId = au.id
               )
GROUP BY au.id, au.firstName, au.lastName, au.creationDate
ORDER BY messageCount DESC, au.id
LIMIT 100
;
