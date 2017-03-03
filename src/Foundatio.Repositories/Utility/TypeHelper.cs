using System;
using System.ComponentModel;
using System.Reflection;

namespace Foundatio.Repositories.Utility {
    public static class TypeHelper {
        public static T ToType<T>(object value) {
            Type targetType = typeof(T);
            if (value == null) {
                try {
                    return (T)Convert.ChangeType(value, targetType);
                } catch {
                    throw new ArgumentNullException(nameof(value));
                }
            }

            TypeConverter converter = TypeDescriptor.GetConverter(targetType);
            Type valueType = value.GetType();

            if (targetType.IsAssignableFrom(valueType))
                return (T)value;

            TypeInfo targetTypeInfo = targetType.GetTypeInfo();
            if (targetTypeInfo.IsEnum && (value is string || valueType.GetTypeInfo().IsEnum)) {
                // attempt to match enum by name.
                if (TryEnumIsDefined<T>(targetType, value.ToString())) {
                    object parsedValue = Enum.Parse(targetType, value.ToString(), false);
                    return (T)parsedValue;
                }

                var message = $"The Enum value of '{value}' is not defined as a valid value for '{targetType.FullName}'.";
                throw new ArgumentException(message);
            }

            if (targetTypeInfo.IsEnum && IsNumeric(valueType))
                return (T)Enum.ToObject(targetType, value);

            if (converter.CanConvertFrom(valueType)) {
                object convertedValue = converter.ConvertFrom(value);
                return (T)convertedValue;
            }

            if (!(value is IConvertible))
                throw new ArgumentException($"An incompatible value specified.  Target Type: {targetType.FullName} Value Type: {value.GetType().FullName}", nameof(value));
            try {
                object convertedValue = Convert.ChangeType(value, targetType);
                return (T)convertedValue;
            } catch (Exception e) {
                throw new ArgumentException($"An incompatible value specified.  Target Type: {targetType.FullName} Value Type: {value.GetType().FullName}", nameof(value), e);
            }
        }

        private static bool TryEnumIsDefined<T>(Type type, object value) {
            // Catch any casting errors that can occur or if 0 is not defined as a default value.
            try {
                if (value is T && Enum.IsDefined(type, (T)value))
                    return true;
            } catch (Exception) { }

            return false;
        }

        private static bool IsNumeric(Type type) {
            if (type.IsArray)
                return false;

            if (type == ByteType ||
                type == DecimalType ||
                type == DoubleType ||
                type == Int16Type ||
                type == Int32Type ||
                type == Int64Type ||
                type == SByteType ||
                type == SingleType ||
                type == UInt16Type ||
                type == UInt32Type ||
                type == UInt64Type)
                return true;

            switch (Type.GetTypeCode(type)) {
                case TypeCode.Byte:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Single:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return true;
            }

            return false;
        }

        private static readonly Type ByteType = typeof(byte);
        private static readonly Type SByteType = typeof(sbyte);
        private static readonly Type SingleType = typeof(float);
        private static readonly Type DecimalType = typeof(decimal);
        private static readonly Type Int16Type = typeof(short);
        private static readonly Type UInt16Type = typeof(ushort);
        private static readonly Type Int32Type = typeof(int);
        private static readonly Type UInt32Type = typeof(uint);
        private static readonly Type Int64Type = typeof(long);
        private static readonly Type UInt64Type = typeof(ulong);
        private static readonly Type DoubleType = typeof(double);
    }
}
