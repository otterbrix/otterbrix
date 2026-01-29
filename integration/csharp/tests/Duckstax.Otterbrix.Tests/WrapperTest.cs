namespace Duckstax.Otterbrix.Tests;

using Duckstax.Otterbrix;

public class Tests
{
    private string GenerateID(int num) {
        string res = num.ToString();
        while (res.Length < 24) {
            res = "0" + res;
        }
        return res;
    }

    // [Test]
    public void Base() {
        OtterbrixWrapper otterbrix = new OtterbrixWrapper(Config.CreateConfig(System.Environment.CurrentDirectory + "/Base"));
        {
            Assert.IsTrue(otterbrix.CreateDatabase("TestDatabase").IsSuccess());
            Assert.IsTrue(otterbrix.CreateCollection("TestDatabase", "TestCollection").IsSuccess());
        }
        {
            string query = "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += ("('" + GenerateID(num + 1) + "', " + "'Name " + num + "', " + num + ")" +
                          (num == 99 ? ";" : ", "));
            }
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.GetError().type == ErrorCode.None);
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 9);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection ORDER BY count;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);

            int index = 0;
            do
            {
                DocumentWrapper doc = cursor.Next();
                Assert.IsTrue(doc.GetLong("count") == index);
                Assert.IsTrue(doc.GetString("name") == "Name " + index.ToString());
                ++index;
            } while (index <= 99);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection ORDER BY count DESC;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);

            int index = 99;
            do
            {
                DocumentWrapper doc = cursor.Next();
                Assert.IsTrue(doc.GetLong("count") == index);
                Assert.IsTrue(doc.GetString("name") == "Name " + index.ToString());
                --index;
            } while (index >= 0);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection  ORDER BY name;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);

            List<int> counts = new List<int>(){0, 1, 10, 11, 12};
            int index = 0;
            do
            {
                DocumentWrapper doc = cursor.Next();
                Assert.IsTrue(doc.GetLong("count") == counts[index]);
                Assert.IsTrue(doc.GetString("name") == "Name " + counts[index].ToString());
                ++index;
            } while (index < counts.Count);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection  WHERE count > 90;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 9);
        }
        {
            string query = "DELETE FROM TestDatabase.TestCollection WHERE count > 90;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 9);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsFalse(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 0);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection WHERE count < 20;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 20);
        }
        {
            string query = "UPDATE TestDatabase.TestCollection SET count = 1000 WHERE count < 20;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 20);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection WHERE count < 20;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsFalse(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 0);
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection WHERE count == 1000;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 20);
        }
    }

    // [Test]
    public void GroupBy() {
        OtterbrixWrapper otterbrix = new OtterbrixWrapper(Config.CreateConfig(System.Environment.CurrentDirectory + "/GroupBy"));
        {
            Assert.IsTrue(otterbrix.CreateDatabase("TestDatabase").IsSuccess());
            Assert.IsTrue(otterbrix.CreateCollection("TestDatabase", "TestCollection").IsSuccess());
        }
        {
            string query = "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += "('" + GenerateID(num + 1) + "', " + "'Name " + (num % 10) + "', " + (num % 20) + ")" +
                         (num == 99 ? ";" : ", ");
            }
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 100);
        }
        {
            string query = "SELECT name, COUNT(count) AS count_, " + "SUM(count) AS sum_, AVG(count) AS avg_, " +
                           "MIN(count) AS min_, MAX(count) AS max_ " + "FROM TestDatabase.TestCollection " +
                           "GROUP BY name;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 10);

            int number = 0;
            do {
                DocumentWrapper doc = cursor.Next();
                Assert.IsTrue(doc.GetString("name") == "Name " + number.ToString());
                Assert.IsTrue(doc.GetLong("count_") == 10);
                Assert.IsTrue(doc.GetLong("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
                Assert.IsTrue(doc.GetDouble("avg_") == (number % 20 + (number + 10) % 20) / 2);
                Assert.IsTrue(doc.GetLong("min_") == number % 20);
                Assert.IsTrue(doc.GetLong("max_") == (number + 10) % 20);
                number++;
            } while (cursor.HasNext());
        }
        {
            string query = "SELECT name, COUNT(count) AS count_, " + "SUM(count) AS sum_, AVG(count) AS avg_, " +
                           "MIN(count) AS min_, MAX(count) AS max_ " + "FROM TestDatabase.TestCollection " +
                           "GROUP BY name " + "ORDER BY name DESC;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 10);

            int number = 9;
            do {
                DocumentWrapper doc = cursor.Next();
                Assert.IsTrue(doc.GetString("name") == "Name " + number.ToString());
                Assert.IsTrue(doc.GetLong("count_") == 10);
                Assert.IsTrue(doc.GetLong("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
                Assert.IsTrue(doc.GetDouble("avg_") == (number % 20 + (number + 10) % 20) / 2);
                Assert.IsTrue(doc.GetLong("min_") == number % 20);
                Assert.IsTrue(doc.GetLong("max_") == (number + 10) % 20);
                number--;
            } while (cursor.HasNext());
        }
    }

    [Test]
    public void InvalidQueries() {
        OtterbrixWrapper otterbrix = new OtterbrixWrapper(Config.CreateConfig(System.Environment.CurrentDirectory + "/InvalidQueries"));
        {
            Assert.IsTrue(otterbrix.CreateDatabase("TestDatabase").IsSuccess());
            Assert.IsTrue(otterbrix.CreateCollection("TestDatabase", "TestCollection").IsSuccess());
        }
        {
            string query = "SELECT * FROM OtherDatabase.OtherCollection;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsFalse(cursor.IsSuccess());
            Assert.IsTrue(cursor.IsError());
            ErrorMessage message = cursor.GetError();
            Assert.IsTrue(message.type == ErrorCode.DatabaseNotExists);
        }
    }

    // [Test]
    public void TestJoin() {
        const string databaseName = "TestDatabase";
        const string collectionName1 = "TestCollection_1";
        const string collectionName2 = "TestCollection_2";

        OtterbrixWrapper otterbrix = new OtterbrixWrapper(Config.CreateConfig(System.Environment.CurrentDirectory + "/TestJoin"));
        {
            Assert.IsTrue(otterbrix.CreateDatabase(databaseName).IsSuccess());
            Assert.IsTrue(otterbrix.CreateCollection(databaseName, collectionName1).IsSuccess());
            Assert.IsTrue(otterbrix.CreateCollection(databaseName, collectionName2).IsSuccess());
        }
        {
            string query = "";
            query += "INSERT INTO " + databaseName + "." + collectionName1
                  + " (_id, name, key_1, key_2) VALUES ";
            for (int num = 0, reversed = 100; num < 101; ++num, --reversed) {
                query += "('" + GenerateID(num + 1) + "', "
                      + "'Name " + num.ToString() + "', " + num.ToString() + ", " + reversed.ToString() + ")" + (reversed == 0 ? ";" : ", ");
            }
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 101);
        }
        {
            string query = "";
            query += "INSERT INTO " + databaseName + "." + collectionName2 + " (_id, value, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += "('" + GenerateID(num + 1001) + "', " + ((num + 25) * 2 * 10).ToString() + ", " + ((num + 25) * 2).ToString() + ")"
                      + (num == 99 ? ";" : ", ");
            }
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 100);
        }
        {
            string query = "";
            query += "SELECT * FROM " + databaseName + "." + collectionName1 + " INNER JOIN " + databaseName
                  + "." + collectionName2 + " ON " + databaseName + "." + collectionName1 + ".key_1"
                  + " = " + databaseName + "." + collectionName2 + ".key"
                  + " ORDER BY key_1 ASC;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 26);

            for (int num = 0; num < 26; ++num) {
                Assert.IsTrue(cursor.HasNext());
                DocumentWrapper doc = cursor.Next();
                Assert.IsTrue(doc.GetLong("key_1") == (num + 25) * 2);
                Assert.IsTrue(doc.GetLong("key") == (num + 25) * 2);
                Assert.IsTrue(doc.GetLong("value") == (num + 25) * 2 * 10);
                Assert.IsTrue(doc.GetString("name") == "Name " + ((num + 25) * 2).ToString());
            }
        }
    }
}