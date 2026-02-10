namespace Duckstax.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class CursorWrapper : IDisposable
    {
        const string libotterbrix = "libotterbrix.so";

        [StructLayout(LayoutKind.Sequential)]
        private struct TransferErrorMessage {
            public int type;
            public IntPtr what;
        }

        [DllImport(libotterbrix, EntryPoint="release_cursor", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseCursor(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_size", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern int CursorSize(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_column_count", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern int CursorColumnCount(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_has_next", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorHasNext(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_is_success", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorIsSuccess(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_is_error", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorIsError(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_get_error", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern TransferErrorMessage CursorGetError(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_column_name", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorColumnName(IntPtr ptr, int columnIndex);

        [DllImport(libotterbrix, EntryPoint="cursor_get_value", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorGetValue(IntPtr ptr, int rowIndex, int columnIndex);

        [DllImport(libotterbrix, EntryPoint="cursor_get_value_by_name", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorGetValueByName(IntPtr ptr, int rowIndex, StringPasser columnName);

        public CursorWrapper(IntPtr cursorStoragePtr) { this.cursorStoragePtr = cursorStoragePtr; }

        ~CursorWrapper() { Dispose(false); }

        public void Dispose() {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing) {
            if (cursorStoragePtr != IntPtr.Zero) {
                ReleaseCursor(cursorStoragePtr);
                cursorStoragePtr = IntPtr.Zero;
            }
        }

        public int Size() { return CursorSize(cursorStoragePtr); }
        public int ColumnCount() { return CursorColumnCount(cursorStoragePtr); }
        public bool HasNext() { return CursorHasNext(cursorStoragePtr); }
        public bool IsSuccess() { return CursorIsSuccess(cursorStoragePtr); }
        public bool IsError() { return CursorIsError(cursorStoragePtr); }

        public ErrorMessage GetError() {
            TransferErrorMessage transfer = CursorGetError(cursorStoragePtr);
            ErrorMessage message = new ErrorMessage();
            message.type = (ErrorCode)transfer.type;
            string? str = Marshal.PtrToStringAnsi(transfer.what);
            message.what = str ?? "";
            Marshal.FreeHGlobal(transfer.what);
            return message;
        }

        public string ColumnName(int columnIndex) {
            IntPtr strPtr = CursorColumnName(cursorStoragePtr, columnIndex);
            if (strPtr == IntPtr.Zero) return "";
            string? result = Marshal.PtrToStringAnsi(strPtr);
            Marshal.FreeHGlobal(strPtr);
            return result ?? "";
        }

        public ValueWrapper GetValue(int rowIndex, int columnIndex) {
            return new ValueWrapper(CursorGetValue(cursorStoragePtr, rowIndex, columnIndex));
        }

        public ValueWrapper GetValue(int rowIndex, string columnName) {
            return new ValueWrapper(CursorGetValueByName(cursorStoragePtr, rowIndex, new StringPasser(ref columnName)));
        }

        private IntPtr cursorStoragePtr;
    }
}