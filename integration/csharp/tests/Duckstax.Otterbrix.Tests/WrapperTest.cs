namespace Duckstax.Otterbrix.Tests;

using Duckstax.Otterbrix;

public class Tests
{
    // [Test]
    public void Base() {
        OtterbrixWrapper otterbrix = new OtterbrixWrapper(Config.CreateConfig(System.Environment.CurrentDirectory + "/Base"));
        {
            Assert.IsTrue(otterbrix.CreateDatabase("TestDatabase").IsSuccess());
            Assert.IsTrue(otterbrix.CreateCollection("TestDatabase", "TestCollection").IsSuccess());
        }
        {
            string query = "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += ("('Name " + num + "', " + num + ")" +
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

            for (int index = 0; index < 100; ++index) {
                using ValueWrapper countVal = cursor.GetValue(index, "count");
                using ValueWrapper nameVal = cursor.GetValue(index, "name");
                Assert.IsTrue(countVal.GetInt() == index);
                Assert.IsTrue(nameVal.GetString() == "Name " + index.ToString());
            }
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection ORDER BY count DESC;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);

            for (int i = 0; i < 100; ++i) {
                int index = 99 - i;
                using ValueWrapper countVal = cursor.GetValue(i, "count");
                using ValueWrapper nameVal = cursor.GetValue(i, "name");
                Assert.IsTrue(countVal.GetInt() == index);
                Assert.IsTrue(nameVal.GetString() == "Name " + index.ToString());
            }
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection ORDER BY name;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);

            List<int> counts = new List<int>(){0, 1, 10, 11, 12};
            for (int index = 0; index < counts.Count; ++index) {
                using ValueWrapper countVal = cursor.GetValue(index, "count");
                using ValueWrapper nameVal = cursor.GetValue(index, "name");
                Assert.IsTrue(countVal.GetInt() == counts[index]);
                Assert.IsTrue(nameVal.GetString() == "Name " + counts[index].ToString());
            }
        }
        {
            string query = "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;";
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
            string query = "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += "('Name " + (num % 10) + "', " + (num % 20) + ")" +
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

            for (int number = 0; number < 10; ++number) {
                using ValueWrapper nameVal = cursor.GetValue(number, "name");
                using ValueWrapper countVal = cursor.GetValue(number, "count_");
                using ValueWrapper sumVal = cursor.GetValue(number, "sum_");
                using ValueWrapper avgVal = cursor.GetValue(number, "avg_");
                using ValueWrapper minVal = cursor.GetValue(number, "min_");
                using ValueWrapper maxVal = cursor.GetValue(number, "max_");
                Assert.IsTrue(nameVal.GetString() == "Name " + number.ToString());
                Assert.IsTrue(countVal.GetInt() == 10);
                Assert.IsTrue(sumVal.GetInt() == 5 * (number % 20) + 5 * ((number + 10) % 20));
                Assert.IsTrue(avgVal.GetDouble() == (number % 20 + (number + 10) % 20) / 2);
                Assert.IsTrue(minVal.GetInt() == number % 20);
                Assert.IsTrue(maxVal.GetInt() == (number + 10) % 20);
            }
        }
        {
            string query = "SELECT name, COUNT(count) AS count_, " + "SUM(count) AS sum_, AVG(count) AS avg_, " +
                           "MIN(count) AS min_, MAX(count) AS max_ " + "FROM TestDatabase.TestCollection " +
                           "GROUP BY name " + "ORDER BY name DESC;";
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 10);

            for (int i = 0; i < 10; ++i) {
                int number = 9 - i;
                using ValueWrapper nameVal = cursor.GetValue(i, "name");
                using ValueWrapper countVal = cursor.GetValue(i, "count_");
                using ValueWrapper sumVal = cursor.GetValue(i, "sum_");
                using ValueWrapper avgVal = cursor.GetValue(i, "avg_");
                using ValueWrapper minVal = cursor.GetValue(i, "min_");
                using ValueWrapper maxVal = cursor.GetValue(i, "max_");
                Assert.IsTrue(nameVal.GetString() == "Name " + number.ToString());
                Assert.IsTrue(countVal.GetInt() == 10);
                Assert.IsTrue(sumVal.GetInt() == 5 * (number % 20) + 5 * ((number + 10) % 20));
                Assert.IsTrue(avgVal.GetDouble() == (number % 20 + (number + 10) % 20) / 2);
                Assert.IsTrue(minVal.GetInt() == number % 20);
                Assert.IsTrue(maxVal.GetInt() == (number + 10) % 20);
            }
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
                  + " (name, key_1, key_2) VALUES ";
            for (int num = 0, reversed = 100; num < 101; ++num, --reversed) {
                query += "('Name " + num.ToString() + "', " + num.ToString() + ", " + reversed.ToString() + ")" + (reversed == 0 ? ";" : ", ");
            }
            CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.Size() == 101);
        }
        {
            string query = "";
            query += "INSERT INTO " + databaseName + "." + collectionName2 + " (value, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += "(" + ((num + 25) * 2 * 10).ToString() + ", " + ((num + 25) * 2).ToString() + ")"
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
                using ValueWrapper key1Val = cursor.GetValue(num, "key_1");
                using ValueWrapper keyVal = cursor.GetValue(num, "key");
                using ValueWrapper valueVal = cursor.GetValue(num, "value");
                using ValueWrapper nameVal = cursor.GetValue(num, "name");
                Assert.IsTrue(key1Val.GetInt() == (num + 25) * 2);
                Assert.IsTrue(keyVal.GetInt() == (num + 25) * 2);
                Assert.IsTrue(valueVal.GetInt() == (num + 25) * 2 * 10);
                Assert.IsTrue(nameVal.GetString() == "Name " + ((num + 25) * 2).ToString());
            }
        }
    }
}