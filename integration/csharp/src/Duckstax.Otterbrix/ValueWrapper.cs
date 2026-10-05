namespace Duckstax.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class ValueWrapper : IDisposable
    {
        const string libotterbrix = OtterbrixWrapper.libotterbrix;

        [DllImport(libotterbrix, EntryPoint="value_is_null", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool ValueIsNull(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_is_bool", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool ValueIsBool(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_is_int", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool ValueIsInt(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_is_uint", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool ValueIsUint(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_is_double", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool ValueIsDouble(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_is_string", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool ValueIsString(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_get_bool", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        [return: MarshalAs(UnmanagedType.I1)]
        private static extern bool ValueGetBool(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_get_int", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern long ValueGetInt(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_get_uint", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern ulong ValueGetUint(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_get_double", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern double ValueGetDouble(ValueHandle value);

        [DllImport(libotterbrix, EntryPoint="value_get_string", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr ValueGetString(ValueHandle value);

        internal ValueWrapper(ValueHandle value) { this.value = value; }

        public void Dispose() { value.Dispose(); }

        public bool IsNull() { return ValueIsNull(value); }
        public bool IsBool() { return ValueIsBool(value); }
        public bool IsInt() { return ValueIsInt(value); }
        public bool IsUint() { return ValueIsUint(value); }
        public bool IsDouble() { return ValueIsDouble(value); }
        public bool IsString() { return ValueIsString(value); }

        public bool GetBool() { return ValueGetBool(value); }
        public long GetInt() { return ValueGetInt(value); }
        public ulong GetUint() { return ValueGetUint(value); }
        public double GetDouble() { return ValueGetDouble(value); }
        public string GetString() {
            return OtterbrixWrapper.TakeString(ValueGetString(value)) ?? "";
        }

        private readonly ValueHandle value;
    }

    internal sealed class ValueHandle : SafeHandle {
        [DllImport(OtterbrixWrapper.libotterbrix, EntryPoint="release_value", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseValue(IntPtr value);

        public ValueHandle() : base(IntPtr.Zero, true) {}
        public override bool IsInvalid => handle == IntPtr.Zero;
        protected override bool ReleaseHandle() {
            ReleaseValue(handle);
            return true;
        }
    }
}
