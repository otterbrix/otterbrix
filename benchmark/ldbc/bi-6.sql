/* Q6. Most authoritative users on a given topic
\set tag '\'Arnold_Schwarzenegger\''
 */
WITH poster_w_liker AS (
        SELECT DISTINCT
            message1.CreatorPersonId AS person1id,
            like2.PersonId AS person2id
        FROM Tag
        JOIN Message_hasTag_Tag
          ON Message_hasTag_Tag.TagId = Tag.id
        JOIN Message message1
          ON message1.MessageId = Message_hasTag_Tag.MessageId
        LEFT JOIN Person_likes_Message like2
               ON like2.MessageId = message1.MessageId
           -- we don't need the Person itself as its ID is in the like
         WHERE Tag.name = :tag
    )
SELECT pl.person1id AS "person1.id",
       sum(coalesce(ps.popularityScore, 0)) AS authorityScore
FROM poster_w_liker pl
LEFT JOIN PopularityScoreQ06 ps
         ON ps.person2id = pl.person2id
GROUP BY pl.person1id
ORDER BY authorityScore DESC, pl.person1id ASC
LIMIT 100
;
