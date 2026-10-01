namespace Duckstax.Otterbrix.Tests;

using Duckstax.Otterbrix;

public class Tests
{
    private static OtterbrixWrapper Open(string name) {
        string path = System.Environment.CurrentDirectory + "/" + name;
        if (Directory.Exists(path)) {
            Directory.Delete(path, true);
        }
        return new OtterbrixWrapper(Config.CreateConfig(path));
    }

    [Test]
    public void StringsComeBackFromTheEngine() {
        using OtterbrixWrapper otterbrix = Open("StringsComeBackFromTheEngine");
        {
            using CursorWrapper cursor = otterbrix.Execute("CREATE DATABASE db;");
            Assert.IsTrue(cursor.IsSuccess());
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("CREATE TABLE db.t (name string);");
            Assert.IsTrue(cursor.IsSuccess());
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("INSERT INTO db.t (name) VALUES ('hello');");
            Assert.IsTrue(cursor.IsSuccess());
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("SELECT name FROM db.t;");
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.ColumnName(0), Is.EqualTo("name"));
            using ValueWrapper value = cursor.GetValue(0, 0);
            Assert.That(value.GetString(), Is.EqualTo("hello"));
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("SELECT * FROM nodb.t;");
            Assert.IsTrue(cursor.IsError());
            Assert.That(cursor.GetError().what, Does.Contain("nodb"));
        }
    }

    [Test]
    public void NonAsciiTextRoundTrips() {
        using OtterbrixWrapper otterbrix = Open("NonAsciiTextRoundTrips");
        {
            using CursorWrapper cursor = otterbrix.Execute("SELECT 'Привет' AS greeting, 7 AS \"число\";");
            Assert.IsTrue(cursor.IsSuccess(), cursor.GetError().what);
            Assert.That(cursor.ColumnName(0), Is.EqualTo("greeting"));
            Assert.That(cursor.ColumnName(1), Is.EqualTo("число"));
            using ValueWrapper greeting = cursor.GetValue(0, 0);
            Assert.That(greeting.GetString(), Is.EqualTo("Привет"));
            using ValueWrapper number = cursor.GetValue(0, "число");
            Assert.IsFalse(number.IsNull());
            Assert.That(number.GetInt(), Is.EqualTo(7));
        }
    }

    [Test]
    public void WritesReportAffectedRows() {
        using OtterbrixWrapper otterbrix = Open("WritesReportAffectedRows");
        {
            using CursorWrapper cursor = otterbrix.Execute("CREATE DATABASE db;");
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.AffectedRows(), Is.Null);
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("CREATE TABLE db.users (name string, age bigint);");
            Assert.IsTrue(cursor.IsSuccess());
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("INSERT INTO db.users (name, age) VALUES ('Alice', 30), ('Bob', 25);");
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(2));
            Assert.That(cursor.Size(), Is.EqualTo(0));
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("UPDATE db.users SET age = 31 WHERE name = 'Alice';");
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(1));
            Assert.That(cursor.Size(), Is.EqualTo(0));
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("DELETE FROM db.users WHERE age > 100;");
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(0));
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("SELECT * FROM db.users;");
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.AffectedRows(), Is.Null);
            Assert.That(cursor.Size(), Is.EqualTo(2));
        }
    }

    [Test]
    public void Base() {
        using OtterbrixWrapper otterbrix = Open("Base");
        {
            using (CursorWrapper created = otterbrix.CreateDatabase("testdatabase")) Assert.IsTrue(created.IsSuccess());
            using (CursorWrapper created = otterbrix.CreateCollection("testdatabase", "testcollection")) Assert.IsTrue(created.IsSuccess());
        }
        {
            string query = "INSERT INTO testdatabase.testcollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += ("('Name " + num + "', " + num + ")" +
                          (num == 99 ? ";" : ", "));
            }
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(100));
        }
        {
            string query = "SELECT * FROM testdatabase.testcollection;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsTrue(cursor.GetError().type == ErrorCode.None);
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 100);
        }
        {
            string query = "SELECT * FROM testdatabase.testcollection WHERE count > 90;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 9);
        }
        {
            string query = "SELECT * FROM testdatabase.testcollection ORDER BY count;";
            using CursorWrapper cursor = otterbrix.Execute(query);
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
            string query = "SELECT * FROM testdatabase.testcollection ORDER BY count DESC;";
            using CursorWrapper cursor = otterbrix.Execute(query);
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
            string query = "SELECT * FROM testdatabase.testcollection ORDER BY name;";
            using CursorWrapper cursor = otterbrix.Execute(query);
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
            string query = "SELECT * FROM testdatabase.testcollection WHERE count > 90;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 9);
        }
        {
            string query = "DELETE FROM testdatabase.testcollection WHERE count > 90;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(9));
        }
        {
            string query = "SELECT * FROM testdatabase.testcollection WHERE count > 90;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 0);
        }
        {
            string query = "SELECT * FROM testdatabase.testcollection WHERE count < 20;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 20);
        }
        {
            string query = "UPDATE testdatabase.testcollection SET count = 1000 WHERE count < 20;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(20));
        }
        {
            string query = "SELECT * FROM testdatabase.testcollection WHERE count < 20;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 0);
        }
        {
            string query = "SELECT * FROM testdatabase.testcollection WHERE count == 1000;";
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.IsFalse(cursor.IsError());
            Assert.IsTrue(cursor.Size() == 20);
        }
    }

    [Test]
    public void GroupBy() {
        using OtterbrixWrapper otterbrix = Open("GroupBy");
        {
            using (CursorWrapper created = otterbrix.CreateDatabase("testdatabase")) Assert.IsTrue(created.IsSuccess());
            using (CursorWrapper created = otterbrix.CreateCollection("testdatabase", "testcollection")) Assert.IsTrue(created.IsSuccess());
        }
        {
            string query = "INSERT INTO testdatabase.testcollection (name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += "('Name " + (num % 10) + "', " + (num % 20) + ")" +
                         (num == 99 ? ";" : ", ");
            }
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(100));
        }
        {
            string query = "SELECT name, COUNT(count) AS count_, " + "SUM(count) AS sum_, AVG(count) AS avg_, " +
                           "MIN(count) AS min_, MAX(count) AS max_ " + "FROM testdatabase.testcollection " +
                           "GROUP BY name;";
            using CursorWrapper cursor = otterbrix.Execute(query);
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
                Assert.IsTrue(countVal.GetUint() == 10);
                Assert.IsTrue(sumVal.GetInt() == 5 * (number % 20) + 5 * ((number + 10) % 20));
                Assert.IsTrue(avgVal.GetDouble() == (number % 20 + (number + 10) % 20) / 2);
                Assert.IsTrue(minVal.GetInt() == number % 20);
                Assert.IsTrue(maxVal.GetInt() == (number + 10) % 20);
            }
        }
        {
            string query = "SELECT name, COUNT(count) AS count_, " + "SUM(count) AS sum_, AVG(count) AS avg_, " +
                           "MIN(count) AS min_, MAX(count) AS max_ " + "FROM testdatabase.testcollection " +
                           "GROUP BY name " + "ORDER BY name DESC;";
            using CursorWrapper cursor = otterbrix.Execute(query);
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
                Assert.IsTrue(countVal.GetUint() == 10);
                Assert.IsTrue(sumVal.GetInt() == 5 * (number % 20) + 5 * ((number + 10) % 20));
                Assert.IsTrue(avgVal.GetDouble() == (number % 20 + (number + 10) % 20) / 2);
                Assert.IsTrue(minVal.GetInt() == number % 20);
                Assert.IsTrue(maxVal.GetInt() == (number + 10) % 20);
            }
        }
    }

    // SQL folds an unquoted name, so the names CreateDatabase / CreateCollection take as written must be lower case.
    [Test]
    public void MixedCaseNamesAreRefused() {
        using OtterbrixWrapper otterbrix = Open("MixedCaseNamesAreRefused");
        {
            using CursorWrapper database = otterbrix.CreateDatabase("TestDatabase");
            Assert.IsTrue(database.IsError());
            Assert.That(database.GetError().type, Is.EqualTo(ErrorCode.InvalidParameter));
            Assert.That(database.GetError().what, Is.EqualTo("create_database: name \"TestDatabase\" must be lower case"));
        }
        {
            using CursorWrapper database = otterbrix.CreateDatabase("testdatabase");
            Assert.IsTrue(database.IsSuccess());
            using CursorWrapper collection = otterbrix.CreateCollection("testdatabase", "TestCollection");
            Assert.IsTrue(collection.IsError());
            Assert.That(collection.GetError().what,
                        Is.EqualTo("create_collection: name \"TestCollection\" must be lower case"));
        }
    }

    [Test]
    public void InvalidQueries() {
        using OtterbrixWrapper otterbrix = Open("InvalidQueries");
        {
            using CursorWrapper database = otterbrix.CreateDatabase("testdatabase");
            Assert.IsTrue(database.IsSuccess());
            using CursorWrapper collection = otterbrix.CreateCollection("testdatabase", "testcollection");
            Assert.IsTrue(collection.IsSuccess());
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("SELECT * FROM OtherDatabase.OtherCollection;");
            Assert.IsFalse(cursor.IsSuccess());
            Assert.IsTrue(cursor.IsError());
            Assert.That(cursor.GetError().type, Is.EqualTo(ErrorCode.DatabaseNotExists));
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("SELECT * FROM testdatabase.OtherCollection;");
            Assert.IsTrue(cursor.IsError());
            Assert.That(cursor.GetError().type, Is.EqualTo(ErrorCode.TableNotExists));
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("SELEC * FROM testdatabase.testcollection;");
            Assert.IsTrue(cursor.IsError());
            Assert.That(cursor.GetError().type, Is.EqualTo(ErrorCode.SqlParseError));
        }
        {
            using CursorWrapper cursor = otterbrix.Execute("SELECT * FROM testdatabase.testcollection;");
            Assert.IsTrue(cursor.IsSuccess(), cursor.GetError().what);
            Assert.That(cursor.GetError().type, Is.EqualTo(ErrorCode.None));
        }
    }

    [Test]
    public void TestJoin() {
        const string databaseName = "testdatabase";
        const string collectionName1 = "testcollection_1";
        const string collectionName2 = "testcollection_2";

        using OtterbrixWrapper otterbrix = Open("TestJoin");
        {
            using (CursorWrapper created = otterbrix.CreateDatabase(databaseName)) Assert.IsTrue(created.IsSuccess());
            using (CursorWrapper created = otterbrix.CreateCollection(databaseName, collectionName1)) Assert.IsTrue(created.IsSuccess());
            using (CursorWrapper created = otterbrix.CreateCollection(databaseName, collectionName2)) Assert.IsTrue(created.IsSuccess());
        }
        {
            string query = "";
            query += "INSERT INTO " + databaseName + "." + collectionName1
                  + " (name, key_1, key_2) VALUES ";
            for (int num = 0, reversed = 100; num < 101; ++num, --reversed) {
                query += "('Name " + num.ToString() + "', " + num.ToString() + ", " + reversed.ToString() + ")" + (reversed == 0 ? ";" : ", ");
            }
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(101));
        }
        {
            string query = "";
            query += "INSERT INTO " + databaseName + "." + collectionName2 + " (value, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query += "(" + ((num + 25) * 2 * 10).ToString() + ", " + ((num + 25) * 2).ToString() + ")"
                      + (num == 99 ? ";" : ", ");
            }
            using CursorWrapper cursor = otterbrix.Execute(query);
            Assert.IsTrue(cursor.IsSuccess());
            Assert.That(cursor.AffectedRows(), Is.EqualTo(100));
        }
        {
            string query = "";
            query += "SELECT * FROM " + databaseName + "." + collectionName1 + " INNER JOIN " + databaseName
                  + "." + collectionName2 + " ON " + databaseName + "." + collectionName1 + ".key_1"
                  + " = " + databaseName + "." + collectionName2 + ".key"
                  + " ORDER BY key_1 ASC;";
            using CursorWrapper cursor = otterbrix.Execute(query);
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