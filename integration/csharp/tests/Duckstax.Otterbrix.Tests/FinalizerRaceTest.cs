namespace Duckstax.Otterbrix.Tests;

using System.Diagnostics;
using Duckstax.Otterbrix;

public class FinalizerRaceTest
{
    private const int Columns = 1500;

    // Bites only where the JIT may end a wrapper's life inside its own native call: `dotnet test -c Release`.
    [Test]
    public void AWrapperIsNotFinalizedUnderItsOwnNativeCall() {
        string path = System.Environment.CurrentDirectory + "/AWrapperIsNotFinalizedUnderItsOwnNativeCall";
        if (Directory.Exists(path)) {
            Directory.Delete(path, true);
        }
        using OtterbrixWrapper otterbrix = new OtterbrixWrapper(Config.CreateConfig(path));
        string text = new string('x', 1000);
        string query = "SELECT " + string.Join(", ", Enumerable.Range(0, Columns).Select(i => "'" + text + "' AS c" + i)) + ";";
        string last = "c" + (Columns - 1);

        bool stop = false;
        Thread collector = new Thread(() => {
            while (!Volatile.Read(ref stop)) {
                GC.Collect(0);
                GC.WaitForPendingFinalizers();
            }
        });
        collector.Start();
        try {
            using CursorWrapper cursor = otterbrix.Execute(query);
            Stopwatch clock = Stopwatch.StartNew();
            while (clock.Elapsed < TimeSpan.FromSeconds(10)) {
                for (int i = 0; i < 100; ++i) {
                    Assert.That(cursor.GetValue(0, last).GetString(), Is.EqualTo(text));
                }
                Assert.That(otterbrix.Execute(query).GetValue(0, last).GetString(), Is.EqualTo(text));
            }
        } finally {
            Volatile.Write(ref stop, true);
            collector.Join();
        }
    }
}
