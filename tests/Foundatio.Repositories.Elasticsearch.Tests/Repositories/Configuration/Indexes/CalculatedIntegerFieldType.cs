using System;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Serializer;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class CalculatedIntegerFieldType : IntegerFieldType
{
    private readonly ScriptService _scriptService;

    public CalculatedIntegerFieldType(ScriptService scriptService)
    {
        _scriptService = scriptService;
    }

    public override async Task<ProcessFieldValueResult> ProcessValueAsync<T>(T document, object value, CustomFieldDefinition fieldDefinition) where T : class
    {
        if (!fieldDefinition.Data.TryGetValue("Expression", out object expression))
            return await base.ProcessValueAsync(document, value, fieldDefinition);

        var calculatedValue = await _scriptService.EvaluateForSourceAsync(document, expression.ToString());

        // TODO: Implement a consecutive errors counter that disables badly behaving expressions
        if (calculatedValue.IsCancelled)
            return new ProcessFieldValueResult { Value = null };

        if (calculatedValue.Value is Double.NaN)
            return new ProcessFieldValueResult { Value = null };

        return new ProcessFieldValueResult { Value = calculatedValue.Value };
    }
}

public class ScriptService
{
    private readonly ITextSerializer _serializer;
    private readonly ILogger<ScriptService> _logger;
    private readonly ConcurrentDictionary<string, string> _registeredExpressions = new();
    private readonly AsyncLock _lock = new();

    public ScriptService(ITextSerializer jsonSerializer, ILogger<ScriptService> logger)
    {
        Engine = CreateEngine();
        _serializer = jsonSerializer;
        _logger = logger;
    }

    public Engine Engine { get; }

    /// <summary>
    /// Thread-safe method that atomically evaluates an expression for a given source document.
    /// The Jint Engine is not thread-safe, so this method uses a lock to ensure only one evaluation occurs at a time.
    /// </summary>
    public async Task<ScriptValueResult> EvaluateForSourceAsync<T>(T source, string expression) where T : class
    {
        using (await _lock.LockAsync().ConfigureAwait(false))
        {
            string functionName = EnsureExpressionFunctionInternal(expression);
            SetSourceInternal(source);
            return GetValueInternal(functionName);
        }
    }

    public string EnsureExpressionFunction(string expression)
    {
        using (_lock.Lock())
        {
            return EnsureExpressionFunctionInternal(expression);
        }
    }

    private string EnsureExpressionFunctionInternal(string expression)
    {
        if (_registeredExpressions.TryGetValue(expression, out string functionName))
            return functionName;

        functionName = "_" + ComputeSha256Hash(expression);
        RegisterFunctionInternal(functionName, expression);

        _registeredExpressions.TryAdd(expression, functionName);

        return functionName;
    }

    public void RegisterFunction(string name, string body)
    {
        using (_lock.Lock())
        {
            RegisterFunctionInternal(name, body);
        }
    }

    private void RegisterFunctionInternal(string name, string body)
    {
        if (String.IsNullOrEmpty(name))
            throw new ArgumentNullException(nameof(name));

        if (String.IsNullOrEmpty(body))
            throw new ArgumentNullException(nameof(body));

        if (!IsValidJavaScriptIdentifier(name))
            throw new ArgumentException("Name must be a valid identifier", nameof(name));

        body = body.Trim();
        string script;
        if (body.StartsWith('{'))
        {
            script = $"function {name}() {body}";
        }
        else if (body.Contains("return "))
        {
            script = $"function {name}() {{ {body}; }}";
        }
        else
        {
            script = $"function {name}() {{ return {body}; }}";
        }

        Engine.Execute(script);
    }

    public void SetSource(object source)
    {
        using (_lock.Lock())
        {
            SetSourceInternal(source);
        }
    }

    private void SetSourceInternal(object source)
    {
        string json = _serializer.SerializeToString(source);
        if (json is null)
            json = "null";

        string script = $"var source = {json};";
        Engine.Execute(script);
    }

    public ScriptValueResult GetValue(string functionName)
    {
        using (_lock.Lock())
        {
            return GetValueInternal(functionName);
        }
    }

    private ScriptValueResult GetValueInternal(string functionName)
    {
        string script = $"{functionName}()";
        try
        {
            var completionValue = Engine.Evaluate(script);
            if (completionValue == JsValue.Undefined)
                return ScriptValueResult.Cancelled;

            return new ScriptValueResult(completionValue.ToObject());
        }
        finally
        {
            Engine.Advanced.ResetCallStack();
        }
    }

    public object ExecuteExpression(string expression)
    {
        using (_lock.Lock())
        {
            return ExecuteExpressionInternal(expression);
        }
    }

    private object ExecuteExpressionInternal(string expression)
    {
        if (String.IsNullOrEmpty(expression))
            throw new ArgumentNullException(nameof(expression));

        try
        {
            return Engine.Evaluate(expression).ToObject();
        }
        finally
        {
            Engine.Advanced.ResetCallStack();
        }
    }

    private static bool IsValidJavaScriptIdentifier(string identifier)
    {
        if (String.IsNullOrEmpty(identifier))
            return false;

        char firstCharacter = identifier[0];
        if (!Char.IsLetter(firstCharacter) && firstCharacter != '_')
            return false;

        return Array.TrueForAll(identifier.ToCharArray(), c => Char.IsLetterOrDigit(c) || c == '_');
    }

    private static string ComputeSha256Hash(string value)
    {
        using var sha256Hash = SHA256.Create();
        byte[] bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(value));

        var builder = new StringBuilder();
        foreach (byte b in bytes)
            builder.Append(b.ToString("x2"));

        return builder.ToString();
    }

    private Engine CreateEngine()
    {
        return new Engine(o =>
        {
            o.LimitRecursion(64);
            o.MaxStatements(100);
            o.Strict();
            o.AddObjectConverter(new JintGuidConverter());
            o.AddObjectConverter(new JintEnumConverter());
            o.AddObjectConverter(new JintDateTimeConverter());
            o.LocalTimeZone(TimeZoneInfo.Utc);
            o.CatchClrExceptions(ex =>
            {
                _logger.LogError(ex, "Error evaluating calculated field expression");
                return false;
            });
        });
    }
}

public class ScriptValueResult
{
    public ScriptValueResult(object value, bool isCancelled = false)
    {
        Value = value;
        IsCancelled = isCancelled;
    }

    public object Value { get; }
    public bool IsCancelled { get; }

    public static readonly ScriptValueResult Cancelled = new(null, true);
}

public class JintEnumConverter : IObjectConverter
{
    public bool TryConvert(Engine engine, object value, out JsValue result)
    {
        if (value.GetType().IsEnum)
        {
            result = value.ToString();
            return true;
        }

        result = null;
        return false;
    }
}

public class JintGuidConverter : IObjectConverter
{
    public bool TryConvert(Engine engine, object value, out JsValue result)
    {
        if (value is Guid guid)
        {
            result = guid.ToString();
            return true;
        }

        result = null;
        return false;
    }
}

public class JintDateTimeConverter : IObjectConverter
{
    public bool TryConvert(Engine engine, object value, out JsValue result)
    {
        if (value is DateTime dateTime)
        {
            result = dateTime.ToString("o");
            return true;
        }

        if (value is DateTimeOffset dateTimeOffset)
        {
            result = dateTimeOffset.ToString("o");
            return true;
        }

        result = null;
        return false;
    }
}
