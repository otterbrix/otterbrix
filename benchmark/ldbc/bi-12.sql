/* Q12. How many persons have a given number of messages
\set startDate '\'2010-07-22\''::timestamp
\set lengthThreshold '20'
\set languages '\'{"ar", "hu"}\''::varchar[]
 */
WITH person_w_posts AS (
    SELECT Person.id, count(Message.MessageId) as messageCount
      FROM Person
      LEFT JOIN Message
        ON Person.id = Message.CreatorPersonId
       AND Message.content IS NOT NULL
       AND Message.length < :lengthThreshold
       AND Message.creationDate > :startDate
       AND Message.RootPostLanguage IN :languages
     GROUP BY Person.id
)
, message_count_distribution AS (
    SELECT pp.messageCount, count(*) as personCount
      FROM person_w_posts pp
     GROUP BY pp.messageCount
     ORDER BY personCount DESC, messageCount DESC
)
SELECT *
  FROM message_count_distribution
ORDER BY personCount DESC, messageCount DESC
;
