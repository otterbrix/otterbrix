namespace Duckstax.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class CursorWrapper : IDisposable
    {
        const string libotterbrix = "otterbrix";

        [StructLayout(LayoutKind.Sequential)]
        private struct TransferErrorMessage {
            public int type;
            public IntPtr what;
        }

        [DllImport(libotterbrix, EntryPoint="cursor_size", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern int CursorSize(CursorHandle cursor);

        [DllImport(libotterbrix, EntryPoint="cursor_affected_rows", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool CursorAffectedRows(CursorHandle cursor, out ulong rows);

        [DllImport(libotterbrix, EntryPoint="cursor_column_count", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern int CursorColumnCount(CursorHandle cursor);

        [DllImport(libotterbrix, EntryPoint="cursor_has_next", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool CursorHasNext(CursorHandle cursor);

        [DllImport(libotterbrix, EntryPoint="cursor_is_success", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool CursorIsSuccess(CursorHandle cursor);

        [DllImport(libotterbrix, EntryPoint="cursor_is_error", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool CursorIsError(CursorHandle cursor);

        [DllImport(libotterbrix, EntryPoint="cursor_get_error", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern TransferErrorMessage CursorGetError(CursorHandle cursor);

        [DllImport(libotterbrix, EntryPoint="cursor_column_name", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorColumnName(CursorHandle cursor, int columnIndex);

        [DllImport(libotterbrix, EntryPoint="cursor_get_value", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern ValueHandle CursorGetValue(CursorHandle cursor, int rowIndex, int columnIndex);

        [DllImport(libotterbrix, EntryPoint="cursor_get_value_by_name", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern ValueHandle CursorGetValueByName(CursorHandle cursor, int rowIndex, StringPasser columnName);

        internal CursorWrapper(CursorHandle cursor) { this.cursor = cursor; }

        public void Dispose() { cursor.Dispose(); }

        public int Size() { return CursorSize(cursor); }
        public ulong? AffectedRows() {
            return CursorAffectedRows(cursor, out ulong rows) ? rows : null;
        }
        public int ColumnCount() { return CursorColumnCount(cursor); }
        public bool HasNext() { return CursorHasNext(cursor); }
        public bool IsSuccess() { return CursorIsSuccess(cursor); }
        public bool IsError() { return CursorIsError(cursor); }

        public ErrorMessage GetError() {
            TransferErrorMessage transfer = CursorGetError(cursor);
            ErrorMessage message = new ErrorMessage();
            message.type = (ErrorCode)transfer.type;
            message.what = OtterbrixWrapper.TakeString(transfer.what) ?? "";
            return message;
        }

        public string ColumnName(int columnIndex) {
            return OtterbrixWrapper.TakeString(CursorColumnName(cursor, columnIndex)) ?? "";
        }

        public ValueWrapper GetValue(int rowIndex, int columnIndex) {
            ValueHandle value = CursorGetValue(cursor, rowIndex, columnIndex);
            if (value.IsInvalid) {
                value.Dispose();
                ThrowIfRowOutOfRange(rowIndex);
                throw new ArgumentOutOfRangeException(nameof(columnIndex), columnIndex,
                    "column " + columnIndex + " is out of range: the cursor has " + ColumnCount() + " columns");
            }
            return new ValueWrapper(value);
        }

        public ValueWrapper GetValue(int rowIndex, string columnName) {
            ValueHandle value = CursorGetValueByName(cursor, rowIndex, new StringPasser(ref columnName));
            if (value.IsInvalid) {
                value.Dispose();
                ThrowIfRowOutOfRange(rowIndex);
                throw new ArgumentException("column \"" + columnName + "\" does not exist", nameof(columnName));
            }
            return new ValueWrapper(value);
        }

        private void ThrowIfRowOutOfRange(int rowIndex) {
            int size = Size();
            if (rowIndex < 0 || rowIndex >= size) {
                throw new ArgumentOutOfRangeException(nameof(rowIndex), rowIndex,
                    "row " + rowIndex + " is out of range: the cursor has " + size + " rows");
            }
        }

        private readonly CursorHandle cursor;
    }

    internal sealed class CursorHandle : SafeHandle {
        [DllImport("otterbrix", EntryPoint="release_cursor", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseCursor(IntPtr cursor);

        public CursorHandle() : base(IntPtr.Zero, true) {}
        public override bool IsInvalid => handle == IntPtr.Zero;
        protected override bool ReleaseHandle() {
            ReleaseCursor(handle);
            return true;
        }
    }
}
