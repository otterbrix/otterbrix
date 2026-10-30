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

    // core::error_code_t, member for member and number for number
    public enum ErrorCode : int {
        None = 0,
        UnimplementedYet = 3,

        TransactionFinalized = 9,
        MissingSavepoint = 10,
        DatabaseAlreadyExists = 13,
        DatabaseNotExists = 14,
        TableAlreadyExists = 15,
        TableNotExists = 16,
        InvalidParameter = 22,

        SqlParseError = 30,
        UnrecognizedFunction = 34,

        OutOfMemory = 38,
        DataCorruption = 39,
        IoError = 40,
        WriteConflict = 41,

        ConnectionClosed = 44,
        ActiveSqlTransaction = 46,
        DependentObjectsStillExist = 47,
        SchemaAlreadyExists = 48,
        SchemaNotExists = 49,
        IndeterminateDatatype = 50,
        AmbiguousParameter = 51,
        DuplicateObject = 52,
        DuplicateColumn = 53,
        UndefinedObject = 54,
        InvalidFunctionDefinition = 55,
        LockFileExists = 56,
        DuplicateFunction = 57,
        WrongObjectType = 58,
        InternalError = 59,
        UndefinedColumn = 60,
        DatatypeMismatch = 61,
        AmbiguousColumn = 62,
        InvalidTableDefinition = 63,
        InvalidObjectDefinition = 64,
        ProtocolViolation = 65,
        NumericValueOutOfRange = 66,
        DivisionByZero = 67,
        CannotCoerce = 68,
        CardinalityViolation = 69,
        InvalidRowCountInLimitClause = 70,
        InvalidRowCountInResultOffsetClause = 71,
        ProgramLimitExceeded = 72,
        InvalidTextRepresentation = 73,
        InvalidDatetimeFormat = 74,
        NullValueNoIndicatorParameter = 75,
        NullValueNotAllowed = 76,
        InvalidRegularExpression = 77,
        AmbiguousFunction = 78,
        GroupingError = 79,
        StatementTooComplex = 80,
        CheckViolation = 81,
        NotNullViolation = 82,
        UniqueViolation = 83,
        ForeignKeyViolation = 84,
        InvalidForeignKey = 85,
        InvalidColumnReference = 86,
        AmbiguousAlias = 87,
        DuplicateAlias = 88,
        IndexCorrupted = 89,
        NoActiveSqlTransaction = 91,
        ObjectNotInPrerequisiteState = 92,
        InsufficientResources = 93,
        UndefinedPreparedStatement = 94,
        DuplicatePreparedStatement = 95,
        UndefinedParameter = 96,
        WindowingError = 97,
    }

    public struct ErrorMessage {
        public ErrorCode type;
        public string what;
        // The 1-based character of the statement text the error points at (PostgreSQL's error position);
        // null when it points at none, and for a refused start.
        public int? position;
    }

    // The C error_message: the caller owns `what` and frees it through Take.
    [StructLayout(LayoutKind.Sequential)]
    internal struct TransferErrorMessage {
        public int type;
        public IntPtr what;

        public ErrorMessage Take() {
            ErrorMessage error = new ErrorMessage();
            error.type = (ErrorCode)type;
            error.what = OtterbrixWrapper.TakeString(what) ?? "";
            return error;
        }
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
        internal const string libotterbrix = "otterbrix";

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

        // The string `native` answers lives as long as `owner`: the marshaller holds `owner` only for the call
        // itself, so this holds it across the copy too, against a finalizer or a Dispose on another thread.
        internal static string BorrowString(SafeHandle owner, Func<IntPtr> native) {
            bool added = false;
            try {
                owner.DangerousAddRef(ref added);
                return Marshal.PtrToStringUTF8(native()) ?? "";
            } finally {
                if (added) {
                    owner.DangerousRelease();
                }
            }
        }

        [DllImport(libotterbrix,
                   EntryPoint = "execute_sql",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern CursorHandle ExecuteSQL(EngineHandle otterbrix, StringPasser sql);

        public OtterbrixWrapper(Config config) {
            otterbrix = OtterbrixCreate(new TransferConfig(ref config), out TransferErrorMessage refusal);
            if (otterbrix.IsInvalid) {
                throw new OtterbrixStartupException(refusal.Take());
            }
        }

        public void Dispose() { otterbrix.Dispose(); }

        public CursorWrapper Execute(string sql) {
            return new CursorWrapper(ExecuteSQL(otterbrix, new StringPasser(ref sql)));
        }

        private readonly EngineHandle otterbrix;
    }

    internal sealed class EngineHandle : SafeHandle {
        [DllImport(OtterbrixWrapper.libotterbrix, EntryPoint="otterbrix_destroy", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void OtterbrixDestroy(IntPtr otterbrix);

        public EngineHandle() : base(IntPtr.Zero, true) {}
        public override bool IsInvalid => handle == IntPtr.Zero;
        protected override bool ReleaseHandle() {
            OtterbrixDestroy(handle);
            return true;
        }
    }
}
