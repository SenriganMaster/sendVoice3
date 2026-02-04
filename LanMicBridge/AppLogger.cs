using System.Globalization;

namespace LanMicBridge;

internal static class AppLogger
{
    private static readonly object Sync = new();
    private static string? _logPath;

    public static void Init(string mode)
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "LanMicBridge",
            "logs");
        Directory.CreateDirectory(baseDir);
        _logPath = Path.Combine(baseDir, DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ".log");
        Log($"起動 Mode={mode}");
    }

    public static void Log(string message)
    {
        if (_logPath == null)
        {
            return;
        }

        var line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] {message}" + Environment.NewLine;
        lock (Sync)
        {
            File.AppendAllText(_logPath, line);
        }
    }

    public static void LogException(string context, Exception ex)
    {
        Log($"{context} Exception={ex.GetType().Name} Message={ex.Message}");
    }
}
