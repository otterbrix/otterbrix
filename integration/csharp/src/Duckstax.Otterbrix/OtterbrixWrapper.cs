namespace Duckstax.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    [StructLayout(LayoutKind.Sequential)]
    public struct StringPasser {
        [MarshalAs(UnmanagedType.LPStr)] public string data;
        public uint size;
        public StringPasser(ref string str) {
            data = str;
            size = (uint) str.Length;
        }
    }

    public enum ErrorCode : int {
        None = 0,
        DatabaseAlreadyExists = 1,
        DatabaseNotExists = 2,
        CollectionAlreadyExists = 3,
        CollectionNotExists = 4,
        CollectionDropped = 5,
        SqlParseError = 6,
        CreatePhisicalPlanError = 7,
        OtherError = -1
    }

    public struct ErrorMessage {
        public ErrorCode type;
        public string what;
    }

    public struct Config {
        public enum LogLevel : int {
            Trace = 0,
            Debug = 1,
            Info = 2,
            Warn = 3,
            Err = 4,
            Critical = 5,
            Off = 6,
        }
        public LogLevel level;
        public string logPath;
        public string walPath;
        public string diskPath;
        public string mainPath;
        public bool walOn;
        public bool syncWalToDisk;

        public Config() {
            level = LogLevel.Trace;
            logPath = System.Environment.CurrentDirectory + "/log";
            walPath = System.Environment.CurrentDirectory + "/wal";
            diskPath = System.Environment.CurrentDirectory + "/disk";
            mainPath = System.Environment.CurrentDirectory;
            walOn = true;
            syncWalToDisk = true;
        }
        // No `disk` parameter: every table is disk-backed, so there is nothing for it to
        // select. TransferConfig below must stay field-for-field with the C `config_t` --
        // LayoutKind.Sequential marshals by position, so a stale field there would shift
        // walOn/syncWalToDisk onto the wrong bytes.
        public Config(LogLevel level,
                      string path,
                      bool wal,
                      bool walDiskSync) {
            this.level = level;
            logPath = path + "/log";
            walPath = path + "/wal";
            diskPath = path + "/disk";
            mainPath = path;
            walOn = wal;
            syncWalToDisk = walDiskSync;
        }
        public static Config DefaultConfig() { return new Config(); }
        public static Config CreateConfig(string path) {
            return new Config(LogLevel.Trace, path, true, true);
        }
    }

    // TODO: Add connection support
    public class OtterbrixWrapper {
        const string libotterbrix = "libotterbrix.so";

        [StructLayout(LayoutKind.Sequential)]
        private struct TransferConfig {
            public int level;
            public StringPasser logPath;
            public StringPasser walPath;
            public StringPasser diskPath;
            public StringPasser mainPath;
            public bool walOn;
            public bool syncWalToDisk;
            public TransferConfig(ref Config config) {
                this.level = (int) config.level;
                this.logPath = new StringPasser(ref config.logPath);
                this.walPath = new StringPasser(ref config.walPath);
                this.diskPath = new StringPasser(ref config.diskPath);
                this.mainPath = new StringPasser(ref config.mainPath);
                this.walOn = config.walOn;
                this.syncWalToDisk = config.syncWalToDisk;
            }
        }

        [DllImport(libotterbrix,
                   EntryPoint = "otterbrix_create",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr
        OtterbrixCreate(TransferConfig config);

        [DllImport(libotterbrix,
                   EntryPoint = "otterbrix_destroy",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern void OtterbrixDestroy(IntPtr otterprixPtr);
        
        [DllImport(libotterbrix,
                   EntryPoint = "execute_sql",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ExecuteSQL(IntPtr otterprixPtr, StringPasser sql);
        [DllImport(libotterbrix,
                   EntryPoint = "create_database",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr CreateDatabase(IntPtr otterprixPtr, StringPasser databaseName);
        [DllImport(libotterbrix,
                   EntryPoint = "create_collection",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr CreateCollection(IntPtr otterprixPtr, StringPasser databaseName, StringPasser collectionName);

        public OtterbrixWrapper(Config config) {
            otterbrixPtr = OtterbrixCreate(new TransferConfig(ref config));
        }
        ~OtterbrixWrapper() { OtterbrixDestroy(otterbrixPtr); }
        public CursorWrapper Execute(string sql) {
            return new CursorWrapper(ExecuteSQL(otterbrixPtr, new StringPasser(ref sql)));
        }
        public CursorWrapper CreateDatabase(string databaseName) {
            return new CursorWrapper(CreateDatabase(otterbrixPtr, new StringPasser(ref databaseName)));
        }
        public CursorWrapper CreateCollection(string databaseName, string collectionName) {
            return new CursorWrapper(CreateCollection(otterbrixPtr, new StringPasser(ref databaseName), new StringPasser(ref collectionName)));
        }

        private readonly IntPtr otterbrixPtr;
    }
}