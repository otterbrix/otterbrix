/* Q2. Tag evolution
\set date '\'2012-06-01\''::timestamp
\set tagClass '\'MusicalArtist\''
 */
WITH
MyTag AS (
SELECT Tag.id AS id, Tag.name AS name
  FROM TagClass
  JOIN Tag
    ON Tag.TypeTagClassId = TagClass.id
 WHERE TagClass.name = :tagClass
),
detail AS (
SELECT t.id as TagId
     , count(CASE WHEN Message.creationDate <  :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth1
     , count(CASE WHEN Message.creationDate >= :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth2
  FROM MyTag t
  JOIN Message_hasTag_Tag
         ON Message_hasTag_tag.TagId = t.id
  JOIN Message
    ON Message.MessageId = Message_hasTag_tag.MessageId
   AND Message.creationDate >= :date
   AND Message.creationDate <  :date + INTERVAL '200 days'
 GROUP BY t.id
)
SELECT t.name AS "tag.name"
     , coalesce(countMonth1, 0)
     , coalesce(countMonth2, 0)
     , abs(coalesce(countMonth1, 0)-coalesce(countMonth2, 0)) AS diff
  FROM MyTag t LEFT JOIN detail ON t.id = detail.TagId
 ORDER BY diff desc, t.name
 LIMIT 100
;
