namespace Duckstax.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    [StructLayout(LayoutKind.Sequential)]
    public struct StringPasser {
        [MarshalAs(UnmanagedType.LPUTF8Str)] public string data;
        public nuint size;
        public StringPasser(ref string str) {
            data = str;
            size = (nuint) System.Text.Encoding.UTF8.GetByteCount(str);
        }
    }

    // core::error_code_t, member for member and in the same order
    public enum ErrorCode : int {
        OtherError = -1,
        None = 0,
        AlreadyExists,
        DoNotExists,
        UnimplementedYet,

        DuplicateField,
        MissingField,
        MissingPrimaryKeyId,
        MissingNamespace,
        TransactionInactive,
        TransactionFinalized,
        MissingSavepoint,
        CommitFailed,
        MissingTable,
        DatabaseAlreadyExists,
        DatabaseNotExists,
        TableAlreadyExists,
        TableNotExists,
        TableDropped,
        TypeAlreadyExists,
        TypeNotExists,
        AmbiguousName,
        FieldNotExists,
        InvalidParameter,

        PhysicalPlanError,
        CreatePhysicalPlanError,

        ArithmeticsFailure,
        ComparisonFailure,
        ConversionFailure,

        IndexCreateFail,
        IndexNotExists,
        SqlParseError,
        SchemaError,
        KernelError,
        FunctionRegistryError,
        UnrecognizedFunction,
        IncorrectFunctionArgument,
        IncorrectFunctionReturnType,
        InvalidConstraint,

        OutOfMemory,
        DataCorruption,
        IoError,
        WriteConflict,
        StaleIndex,

        ActorAgentMissing,
        ConnectionClosed,
    }

    public struct ErrorMessage {
        public ErrorCode type;
        public string what;
    }

    // Thrown by the OtterbrixWrapper constructor when the engine refuses to start.
    public class OtterbrixStartupException : Exception {
        public OtterbrixStartupException(ErrorMessage error)
            : base(error.what) { Error = error; }
        public ErrorMessage Error { get; }
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

        public Config() {
            level = LogLevel.Trace;
            logPath = System.Environment.CurrentDirectory + "/log";
            walPath = System.Environment.CurrentDirectory + "/wal";
            diskPath = System.Environment.CurrentDirectory + "/disk";
            mainPath = System.Environment.CurrentDirectory;
        }
        // No `disk`, `walDiskSync` or `wal` parameter: every table is disk-backed, every commit
        // fsyncs, and the journal is not optional. TransferConfig below must stay field-for-field
        // with the C `config_t` -- LayoutKind.Sequential marshals by position, so a stale field
        // there would land on the wrong bytes with nothing to report it.
        public Config(LogLevel level, string path) {
            this.level = level;
            logPath = path + "/log";
            walPath = path + "/wal";
            diskPath = path + "/disk";
            mainPath = path;
        }
        public static Config DefaultConfig() { return new Config(); }
        public static Config CreateConfig(string path) {
            return new Config(LogLevel.Trace, path);
        }
    }

    // TODO: Add connection support
    public class OtterbrixWrapper : IDisposable {
        const string libotterbrix = "otterbrix";

        [StructLayout(LayoutKind.Sequential)]
        private struct TransferConfig {
            public int level;
            public StringPasser logPath;
            public StringPasser walPath;
            public StringPasser diskPath;
            public StringPasser mainPath;
            public TransferConfig(ref Config config) {
                this.level = (int) config.level;
                this.logPath = new StringPasser(ref config.logPath);
                this.walPath = new StringPasser(ref config.walPath);
                this.diskPath = new StringPasser(ref config.diskPath);
                this.mainPath = new StringPasser(ref config.mainPath);
            }
        }

        [DllImport(libotterbrix,
                   EntryPoint = "otterbrix_create",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern EngineHandle
        OtterbrixCreate(TransferConfig config, out TransferErrorMessage error);

        [StructLayout(LayoutKind.Sequential)]
        private struct TransferErrorMessage {
            public int type;
            public IntPtr what;
        }

        [DllImport(libotterbrix,
                   EntryPoint = "otterbrix_free_string",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern void OtterbrixFreeString(IntPtr str);

        internal static string? TakeString(IntPtr str) {
            string? result = Marshal.PtrToStringUTF8(str);
            OtterbrixFreeString(str);
            return result;
        }

        [DllImport(libotterbrix,
                   EntryPoint = "execute_sql",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern CursorHandle ExecuteSQL(EngineHandle otterbrix, StringPasser sql);

        public OtterbrixWrapper(Config config) {
            otterbrix = OtterbrixCreate(new TransferConfig(ref config), out TransferErrorMessage refusal);
            if (otterbrix.IsInvalid) {
                ErrorMessage error = new ErrorMessage();
                error.type = (ErrorCode)refusal.type;
                error.what = TakeString(refusal.what) ?? "";
                throw new OtterbrixStartupException(error);
            }
        }

        public void Dispose() { otterbrix.Dispose(); }

        public CursorWrapper Execute(string sql) {
            return new CursorWrapper(ExecuteSQL(otterbrix, new StringPasser(ref sql)));
        }

        private readonly EngineHandle otterbrix;
    }

    internal sealed class EngineHandle : SafeHandle {
        [DllImport("otterbrix", EntryPoint="otterbrix_destroy", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void OtterbrixDestroy(IntPtr otterbrix);

        public EngineHandle() : base(IntPtr.Zero, true) {}
        public override bool IsInvalid => handle == IntPtr.Zero;
        protected override bool ReleaseHandle() {
            OtterbrixDestroy(handle);
            return true;
        }
    }
}
