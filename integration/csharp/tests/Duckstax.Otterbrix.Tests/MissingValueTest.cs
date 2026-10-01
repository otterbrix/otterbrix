namespace Duckstax.Otterbrix.Tests;

using Duckstax.Otterbrix;

public class MissingValueTest
{
    private static OtterbrixWrapper Open(string name) {
        string path = System.Environment.CurrentDirectory + "/" + name;
        if (Directory.Exists(path)) {
            Directory.Delete(path, true);
        }
        return new OtterbrixWrapper(Config.CreateConfig(path));
    }

    [Test]
    public void AMissingColumnNameIsRefused() {
        using OtterbrixWrapper otterbrix = Open("AMissingColumnNameIsRefused");
        using CursorWrapper cursor = otterbrix.Execute("SELECT 'kept' AS name;");
        ArgumentException refused = Assert.Throws<ArgumentException>(() => cursor.GetValue(0, "no_such_column").GetString())!;
        Assert.That(refused.ParamName, Is.EqualTo("columnName"));
        Assert.That(refused.Message, Does.StartWith("column \"no_such_column\" does not exist"));
    }

    [TestCase(1, 0, "rowIndex")]
    [TestCase(-1, 0, "rowIndex")]
    [TestCase(0, 1, "columnIndex")]
    [TestCase(0, -1, "columnIndex")]
    public void AnIndexOutOfRangeIsRefused(int row, int column, string parameter) {
        using OtterbrixWrapper otterbrix = Open("AnIndexOutOfRangeIsRefused");
        using CursorWrapper cursor = otterbrix.Execute("SELECT 'kept' AS name;");
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(() => cursor.GetValue(row, column).GetString())!;
        Assert.That(refused.ParamName, Is.EqualTo(parameter));
    }

    [Test]
    public void ARowOutOfRangeIsRefusedByName() {
        using OtterbrixWrapper otterbrix = Open("ARowOutOfRangeIsRefusedByName");
        using CursorWrapper cursor = otterbrix.Execute("SELECT 'kept' AS name;");
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(() => cursor.GetValue(1, "name").GetString())!;
        Assert.That(refused.ParamName, Is.EqualTo("rowIndex"));
    }
}
