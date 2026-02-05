using System.Text.Json;
using System.Text.Json.Serialization;

namespace LanMicBridge;

internal sealed class AppSettings
{
    public string? LastMode { get; set; }
    public string? OutputDeviceId { get; set; }
    public string? OutputDeviceName { get; set; }
    public int? OutputDeviceIndex { get; set; }
    public int? JitterIndex { get; set; }
    public int? OutputGainPercent { get; set; }
    public int? OutputForceStartMs { get; set; }
    public bool? RecvProcessingEnabled { get; set; }
    public bool? ReceiverDetailVisible { get; set; }
    public string? SenderIp { get; set; }
    public int? CaptureApiIndex { get; set; }
    public string? CaptureDeviceId { get; set; }
    public string? CaptureDeviceName { get; set; }
    public int? CaptureDeviceIndex { get; set; }
    public int? CaptureMmeIndex { get; set; }
    public int? QualityIndex { get; set; }
    public int? SendModeIndex { get; set; }
    public int? SendGainPercent { get; set; }
    public bool? SendProcessingEnabled { get; set; }
    public bool? SendTestToneEnabled { get; set; }
    public bool? SenderDetailVisible { get; set; }
    public float? VadThresholdDb { get; set; }
    public bool? SettingsVisible { get; set; }
    public int? SettingsTabIndex { get; set; }

    public static AppSettings Load()
    {
        try
        {
            var path = GetSettingsPath();
            if (!File.Exists(path))
            {
                return new AppSettings();
            }

            var json = File.ReadAllText(path);
            return JsonSerializer.Deserialize<AppSettings>(json) ?? new AppSettings();
        }
        catch (Exception ex)
        {
            AppLogger.LogException("設定読み込み失敗", ex);
            return new AppSettings();
        }
    }

    public void Save()
    {
        try
        {
            var path = GetSettingsPath();
            var dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrWhiteSpace(dir))
            {
                Directory.CreateDirectory(dir);
            }

            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };
            var json = JsonSerializer.Serialize(this, options);
            File.WriteAllText(path, json);
        }
        catch (Exception ex)
        {
            AppLogger.LogException("設定保存失敗", ex);
        }
    }

    private static string GetSettingsPath()
    {
        return Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "LanMicBridge",
            "settings.json");
    }
}
