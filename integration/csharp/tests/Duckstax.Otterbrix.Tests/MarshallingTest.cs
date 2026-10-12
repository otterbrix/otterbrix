namespace Duckstax.Otterbrix.Tests;

using System.Reflection;
using System.Runtime.InteropServices;
using Duckstax.Otterbrix;

public class MarshallingTest
{
    private static IEnumerable<MethodInfo> Imports() {
        return typeof(OtterbrixWrapper).Assembly.GetTypes()
            .SelectMany(type => type.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
            .Where(method => method.GetCustomAttribute<DllImportAttribute>() != null);
    }

    private static bool IsOneByte(MarshalAsAttribute? marshalAs) {
        return marshalAs != null && marshalAs.Value == UnmanagedType.I1;
    }

    [Test]
    public void EveryCBoolIsMarshalledAsOneByte() {
        List<string> wide = new List<string>();
        foreach (MethodInfo method in Imports()) {
            IEnumerable<ParameterInfo> bools = method.GetParameters().Append(method.ReturnParameter)
                .Where(parameter => parameter.ParameterType == typeof(bool) ||
                                    parameter.ParameterType == typeof(bool).MakeByRefType());
            foreach (ParameterInfo parameter in bools) {
                if (!IsOneByte(parameter.GetCustomAttribute<MarshalAsAttribute>())) {
                    wide.Add(method.DeclaringType!.Name + "." + method.Name + " " +
                             (parameter.Position < 0 ? "return" : parameter.Name));
                }
            }
        }
        Assert.That(Imports(), Is.Not.Empty);
        Assert.That(wide, Is.Empty);
    }

    [Test]
    public void EveryCBoolFieldIsMarshalledAsOneByte() {
        IEnumerable<Type> structs = Imports()
            .SelectMany(method => method.GetParameters().Append(method.ReturnParameter))
            .Select(parameter => parameter.ParameterType.IsByRef ? parameter.ParameterType.GetElementType()! : parameter.ParameterType)
            .Where(type => type.IsValueType && !type.IsPrimitive && !type.IsEnum)
            .Distinct();
        List<string> wide = new List<string>();
        foreach (Type type in structs) {
            foreach (FieldInfo field in type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)) {
                if (field.FieldType == typeof(bool) && !IsOneByte(field.GetCustomAttribute<MarshalAsAttribute>())) {
                    wide.Add(type.Name + "." + field.Name);
                }
            }
        }
        Assert.That(structs, Is.Not.Empty);
        Assert.That(wide, Is.Empty);
    }
}
