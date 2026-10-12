namespace Duckstax.Otterbrix.Tests;

using Duckstax.Otterbrix;

public class MissingValueTest
{
    [Test]
    public void AMissingColumnNameIsRefused() {
        using OtterbrixWrapper otterbrix = TestDirectory.OpenFresh("AMissingColumnNameIsRefused");
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
        using OtterbrixWrapper otterbrix = TestDirectory.OpenFresh("AnIndexOutOfRangeIsRefused");
        using CursorWrapper cursor = otterbrix.Execute("SELECT 'kept' AS name;");
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(() => cursor.GetValue(row, column).GetString())!;
        Assert.That(refused.ParamName, Is.EqualTo(parameter));
    }

    [Test]
    public void ARowOutOfRangeIsRefusedByName() {
        using OtterbrixWrapper otterbrix = TestDirectory.OpenFresh("ARowOutOfRangeIsRefusedByName");
        using CursorWrapper cursor = otterbrix.Execute("SELECT 'kept' AS name;");
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(() => cursor.GetValue(1, "name").GetString())!;
        Assert.That(refused.ParamName, Is.EqualTo("rowIndex"));
    }
}
