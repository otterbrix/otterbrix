namespace Duckstax.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class ValueWrapper : IDisposable
    {
        const string libotterbrix = "libotterbrix.so";

        [DllImport(libotterbrix, EntryPoint="release_value", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseValue(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_is_null", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool ValueIsNull(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_is_bool", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool ValueIsBool(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_is_int", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool ValueIsInt(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_is_uint", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool ValueIsUint(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_is_double", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool ValueIsDouble(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_is_string", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool ValueIsString(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_get_bool", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool ValueGetBool(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_get_int", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern long ValueGetInt(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_get_uint", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern ulong ValueGetUint(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_get_double", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern double ValueGetDouble(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="value_get_string", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr ValueGetString(IntPtr ptr);

        public ValueWrapper(IntPtr valuePtr) { this.valuePtr = valuePtr; }

        ~ValueWrapper() { Dispose(false); }

        public void Dispose() {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing) {
            if (valuePtr != IntPtr.Zero) {
                ReleaseValue(valuePtr);
                valuePtr = IntPtr.Zero;
            }
        }

        public bool IsNull() { return valuePtr == IntPtr.Zero || ValueIsNull(valuePtr); }
        public bool IsBool() { return ValueIsBool(valuePtr); }
        public bool IsInt() { return ValueIsInt(valuePtr); }
        public bool IsUint() { return ValueIsUint(valuePtr); }
        public bool IsDouble() { return ValueIsDouble(valuePtr); }
        public bool IsString() { return ValueIsString(valuePtr); }

        public bool GetBool() { return ValueGetBool(valuePtr); }
        public long GetInt() { return ValueGetInt(valuePtr); }
        public ulong GetUint() { return ValueGetUint(valuePtr); }
        public double GetDouble() { return ValueGetDouble(valuePtr); }
        public string GetString() {
            IntPtr strPtr = ValueGetString(valuePtr);
            string? result = Marshal.PtrToStringAnsi(strPtr);
            Marshal.FreeHGlobal(strPtr);
            return result ?? "";
        }

        private IntPtr valuePtr;
    }
}