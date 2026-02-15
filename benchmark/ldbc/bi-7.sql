/* Q7. Related topics
\set tag '\'Enrique_Iglesias\''
 */
WITH MyMessage AS (
  SELECT m.MessageId
  FROM Message_hasTag_Tag m, Tag
  WHERE Tag.name = :tag and m.TagId = Tag.Id
)
SELECT RelatedTag.name AS "relatedTag.name"
     , count(*) AS count
  FROM MyMessage ParentMessage_HasTag_Tag
  -- as an optimization, we don't need message here as its ID is in ParentMessage_HasTag_Tag
  -- so proceed to the comment directly
  INNER JOIN Message Comment
          ON ParentMessage_HasTag_Tag.MessageId = Comment.ParentMessageId
  -- comment's tag
  LEFT  JOIN Message_hasTag_Tag ct
          ON Comment.MessageId = ct.MessageId
  INNER JOIN Tag RelatedTag
          ON RelatedTag.id = ct.TagId
 WHERE TRUE
  -- comment doesn't have the given tag
   AND Comment.MessageId NOT IN (SELECT MessageId FROM MyMessage)
   AND Comment.ParentMessageId IS NOT NULL
 GROUP BY RelatedTag.Name
 ORDER BY count DESC, RelatedTag.name
 LIMIT 100
;
