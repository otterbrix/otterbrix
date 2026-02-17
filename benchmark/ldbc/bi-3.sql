/* Q3. Popular topics in a country
\set tagClass '\'MusicalArtist\''
\set country '\'Burma\''
 */
SELECT Forum.id                AS "forum.id"
     , Forum.title             AS "forum.title"
     , Forum.creationDate      AS "forum.creationDate"
     , Forum.ModeratorPersonId AS "person.id"
     , count(Message.MessageId) AS messageCount
FROM Message
JOIN Forum
  ON Forum.id = Message.ContainerForumId
JOIN Person AS ModeratorPerson
  ON ModeratorPerson.id = Forum.ModeratorPersonId
JOIN City
  ON City.id = ModeratorPerson.LocationCityId
JOIN Country
  ON Country.id = City.PartOfCountryId
 AND Country.name = :country
WHERE EXISTS (
  SELECT 1
    FROM TagClass
    JOIN Tag
      ON Tag.TypeTagClassId = TagClass.id
    JOIN Message_hasTag_Tag
      ON Message_hasTag_Tag.TagId = Tag.id
   WHERE Message.MessageId = Message_hasTag_Tag.MessageId AND TagClass.name = :tagClass)
GROUP BY Forum.id, Forum.title, Forum.creationDate, Forum.ModeratorPersonId
ORDER BY messageCount DESC, Forum.id
LIMIT 20
;
