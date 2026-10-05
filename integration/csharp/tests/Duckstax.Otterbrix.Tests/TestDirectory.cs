namespace Duckstax.Otterbrix.Tests;

using Duckstax.Otterbrix;

internal static class TestDirectory
{
    // An empty directory named after the test, under the working directory.
    public static string Fresh(string name) {
        string path = System.Environment.CurrentDirectory + "/" + name;
        if (System.IO.Directory.Exists(path)) {
            System.IO.Directory.Delete(path, true);
        }
        return path;
    }

    public static OtterbrixWrapper OpenFresh(string name) {
        return new OtterbrixWrapper(Config.CreateConfig(Fresh(name)));
    }
}
