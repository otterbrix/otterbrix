/* Q10. Experts in social circle
\set personId 30786325588624
\set country '\'China\''
\set tagClass '\'MusicalArtist\''
\set minPathDistance 3 -- fixed value
\set maxPathDistance 4 -- fixed value
 */
WITH friends AS
  (SELECT Person2Id
     FROM Person_knows_Person
    WHERE Person1Id = :personId
  )
  , friends_of_friends AS
  (SELECT knowsB.Person2Id AS Person2Id
     FROM friends
     JOIN Person_knows_Person knowsB
       ON friends.Person2Id = knowsB.Person1Id
  )
  , friends_and_friends_of_friends AS
  (SELECT Person2Id
     FROM friends
    UNION -- using plain UNION to eliminate duplicates
   SELECT Person2Id
     FROM friends_of_friends
  )
  , friends_between_3_and_4_hops AS (
    -- people reachable through 1..4 hops
    (SELECT DISTINCT knowsD.Person2Id AS Person2Id
      FROM friends_and_friends_of_friends ffoaf
      JOIN Person_knows_Person knowsC
        ON knowsC.Person1Id = ffoaf.Person2Id
      JOIN Person_knows_Person knowsD
        ON knowsD.Person1Id = knowsC.Person2Id
    )
    -- removing people reachable through 1..2 hops, yielding the ones reachable through 3..4 hops
    EXCEPT
    (SELECT Person2Id
      FROM friends_and_friends_of_friends
    )
  )
  , friend_list AS (
    SELECT f.person2Id AS friendId
      FROM friends_between_3_and_4_hops f
      JOIN Person tf -- the friend's person record
        ON tf.id = f.person2Id
      JOIN City
        ON City.id = tf.LocationCityId
      JOIN Country
        ON Country.id = City.PartOfCountryId
       AND Country.name = :country
  )
  , messages_of_tagclass_by_friends AS (
    SELECT DISTINCT f.friendId
         , Message.MessageId AS messageid
      FROM friend_list f
      JOIN Message
        ON Message.CreatorPersonId = f.friendId
      JOIN Message_hasTag_Tag
        ON Message_hasTag_Tag.MessageId = Message.MessageId
      JOIN Tag
        ON Tag.id = Message_hasTag_Tag.TagId
      JOIN TagClass
        ON TagClass.id = Tag.TypeTagClassId
      WHERE TagClass.name = :tagClass
  )
SELECT m.friendId AS "person.id"
     , Tag.name AS "tag.name"
     , count(*) AS messageCount
  FROM messages_of_tagclass_by_friends m
  JOIN Message_hasTag_Tag
    ON Message_hasTag_Tag.MessageId = m.MessageId
  JOIN Tag
    ON Tag.id = Message_hasTag_Tag.TagId
 GROUP BY m.friendId, Tag.name
 ORDER BY messageCount DESC, Tag.name, m.friendId
 LIMIT 100
;
