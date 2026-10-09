namespace Duckstax.Otterbrix.Tests;

using System.Runtime.CompilerServices;
using Duckstax.Otterbrix;

public class LifetimeTest
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static CursorWrapper SelectFromAnEngineNobodyHolds(string path) {
        OtterbrixWrapper otterbrix = new OtterbrixWrapper(Config.CreateConfig(path));
        using (CursorWrapper created = otterbrix.Execute("CREATE DATABASE db;")) Assert.IsTrue(created.IsSuccess());
        using (CursorWrapper created = otterbrix.Execute("CREATE TABLE db.t (name string);")) Assert.IsTrue(created.IsSuccess());
        using (CursorWrapper inserted = otterbrix.Execute("INSERT INTO db.t (name) VALUES ('kept');")) Assert.IsTrue(inserted.IsSuccess());
        return otterbrix.Execute("SELECT name FROM db.t;");
    }

    [Test]
    public void ACursorOutlivesItsFinalizedEngine() {
        string path = TestDirectory.Fresh("ACursorOutlivesItsFinalizedEngine");
        using (CursorWrapper cursor = SelectFromAnEngineNobodyHolds(path)) {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            Assert.That(cursor.Size(), Is.EqualTo(1));
            Assert.That(cursor.ColumnName(0), Is.EqualTo("name"));
            using ValueWrapper value = cursor.GetValue(0, 0);
            Assert.That(value.GetString(), Is.EqualTo("kept"));
            Assert.Throws<OtterbrixStartupException>(() => new OtterbrixWrapper(Config.CreateConfig(path)));
        }
        using OtterbrixWrapper reopened = new OtterbrixWrapper(Config.CreateConfig(path));
        using CursorWrapper again = reopened.Execute("SELECT name FROM db.t;");
        Assert.That(again.Size(), Is.EqualTo(1));
    }

    [Test]
    public void ACursorOutlivesItsDisposedEngine() {
        string path = TestDirectory.Fresh("ACursorOutlivesItsDisposedEngine");
        OtterbrixWrapper otterbrix = new OtterbrixWrapper(Config.CreateConfig(path));
        using (CursorWrapper cursor = otterbrix.Execute("SELECT 'kept' AS name;")) {
            otterbrix.Dispose();
            otterbrix.Dispose();
            Assert.Throws<ObjectDisposedException>(() => otterbrix.Execute("SELECT 1;"));

            using ValueWrapper value = cursor.GetValue(0, "name");
            Assert.That(value.GetString(), Is.EqualTo("kept"));
        }
        using OtterbrixWrapper reopened = new OtterbrixWrapper(Config.CreateConfig(path));
    }
}
