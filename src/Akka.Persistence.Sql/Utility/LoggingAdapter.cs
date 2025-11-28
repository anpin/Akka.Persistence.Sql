
using System.Diagnostics;
using LinqToDB.Data;
using Akka.Event;
namespace Akka.Persistence.Sql.Utility;


public class LinqToDBLoggerAdapter
{
    private readonly ILoggingAdapter _akkaLogger;

    public LinqToDBLoggerAdapter(ILoggingAdapter akkaLogger)
    {

        _akkaLogger = akkaLogger;
    }
    static LogLevel ConvertLevel(TraceLevel level) => level switch
    {
        TraceLevel.Error =>  LogLevel.ErrorLevel,
        TraceLevel.Info => LogLevel.InfoLevel,
        TraceLevel.Verbose => LogLevel.DebugLevel,
        TraceLevel.Warning => LogLevel.WarningLevel,
        _ => LogLevel.DebugLevel,
    };
    public void OnTrace(TraceInfo traceInfo) =>
        _akkaLogger.Log(ConvertLevel(traceInfo.TraceLevel), traceInfo.Exception,
            "[StartTime: {StartTime}][ExecutionTime: {ExecutionTime}][Sql: {SqlText}]"
            , traceInfo.StartTime
            , traceInfo.ExecutionTime
            , traceInfo.SqlText
            );

    public void OnWrite(string? arg1, string? arg2, TraceLevel traceLevel) =>
        _akkaLogger.Log(ConvertLevel(traceLevel), "[{0}][{1}]", new object[]  { arg1, arg2 } );

}

