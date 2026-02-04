
using System.Diagnostics;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Concentus.Enums;
using Concentus.Structs;
using NAudio.CoreAudioApi;
using NAudio.Wave;

namespace LanMicBridge;

public partial class Form1 : Form
{
    private const int SampleRate = 48000;
    private const int Channels = 1;
    private const int FrameMs = 20;
    private const int FrameSamples = SampleRate * FrameMs / 1000;
    private const int DefaultPort = 48750;
    private const int KeepAliveMs = 1000;
    private const float VadThresholdDb = -45f;
    private const int VadHangoverMs = 300;
    private const float ClipThresholdLinear = 0.89f;

    private readonly AudioMeter _meterA = new();
    private readonly AudioMeter _meterB = new();
    private readonly Stopwatch _statsWatch = new();
    private readonly Stopwatch _receiverClock = new();

    private MMDeviceEnumerator? _deviceEnumerator;
    private readonly List<MMDevice> _renderDevices = new();
    private readonly List<MMDevice> _captureDevices = new();

    private WasapiOut? _output;
    private BufferedWaveProvider? _playBuffer;
    private UdpClient? _receiverUdp;
    private CancellationTokenSource? _receiverCts;
    private Task? _receiverTask;
    private OpusDecoder? _decoder;
    private uint _expectedSequence;
    private bool _hasSequence;
    private long _packetsReceived;
    private long _packetsLost;
    private long _bytesReceived;
    private double _jitterMs;
    private double _lastTransitMs;
    private DateTime _lastPacketTime = DateTime.MinValue;
    private bool _hadConnection;
    private IPEndPoint? _lastSenderEndpoint;
    private DateTime _suppressNetworkAudioUntil = DateTime.MinValue;
    private DateTime? _lowRmsSince;
    private string _lastReceiverStatus = string.Empty;
    private string _lastSenderStatus = string.Empty;
    private int _senderReconnectCount;
    private int _senderDisconnectCount;
    private DateTime _lastStatsLogged = DateTime.MinValue;
    private float _outputGain = 1.0f;
    private DateTime _lastSenderMeterUpdate = DateTime.MinValue;

    private WasapiCapture? _capture;
    private BufferedWaveProvider? _captureBuffer;
    private MediaFoundationResampler? _resampler;
    private OpusEncoder? _encoder;
    private UdpClient? _senderUdp;
    private CancellationTokenSource? _senderCts;
    private Task? _senderTask;
    private Task? _senderReceiveTask;
    private uint _senderId;
    private uint _sendSequence;
    private DateTime _lastKeepAlive = DateTime.MinValue;
    private DateTime _lastHello = DateTime.MinValue;
    private DateTime _lastAccept = DateTime.MinValue;
    private bool _accepted;
    private DateTime _lastVoiceTime = DateTime.MinValue;
    private int _selectedBitrate = 32000;
    private int _selectedComplexity = 5;
    private float _sendGain = 1.0f;

    private System.Windows.Forms.Timer _statsTimer = null!;
    private System.Windows.Forms.Timer _receiverStatusTimer = null!;
    private System.Windows.Forms.Timer _silenceTimer = null!;

    private RadioButton _radioReceiver = null!;
    private RadioButton _radioSender = null!;
    private Panel _receiverPanel = null!;
    private Panel _senderPanel = null!;
    private Label _lblIpList = null!;
    private Button _btnCopyIp = null!;
    private Label _lblPort = null!;
    private Label _lblCableStatus = null!;
    private Label _lblCableGuide = null!;
    private ProgressBar _progressMeterA = null!;
    private ProgressBar _progressMeterB = null!;
    private Label _lblMeterA = null!;
    private Label _lblMeterB = null!;
    private Label _lblMeterWarning = null!;
    private Label _lblMeterGuide = null!;
    private Label _lblStats = null!;
    private Button _btnCheckTone = null!;
    private Label _lblCheckResult = null!;
    private Label _lblAquaGuide = null!;
    private LinkLabel _linkReceiverDetail = null!;
    private GroupBox _groupReceiverDetail = null!;
    private ComboBox _comboOutputDevice = null!;
    private ComboBox _comboJitter = null!;
    private TrackBar _trackOutputGain = null!;
    private Label _lblOutputGainValue = null!;
    private Label _lblOutputLevel = null!;

    private TextBox _txtIp = null!;
    private Button _btnSenderToggle = null!;
    private Label _lblSenderStatus = null!;
    private LinkLabel _linkSenderDetail = null!;
    private GroupBox _groupSenderDetail = null!;
    private ComboBox _comboMicDevice = null!;
    private ComboBox _comboQuality = null!;
    private TrackBar _trackGain = null!;
    private Label _lblGainValue = null!;
    private Label _lblSenderMeter = null!;
    private Label _lblSenderMeterDetail = null!;

    private StatusStrip _statusStrip = null!;
    private ToolStripStatusLabel _statusLabel = null!;

    public Form1()
    {
        InitializeComponent();
        BuildUi();
        _senderId = (uint)Random.Shared.Next(1, int.MaxValue);
    }

    private void Form1_Load(object? sender, EventArgs e)
    {
        AppLogger.Init("Receiver");
        RefreshDeviceLists();
        UpdateIpList();
        UpdateCableStatus();
        StartReceiver();
        UpdateModeUi(true);
    }

    private void Form1_FormClosing(object? sender, FormClosingEventArgs e)
    {
        StopSender();
        StopReceiver();
        _deviceEnumerator?.Dispose();
        _statsTimer?.Stop();
        _receiverStatusTimer?.Stop();
        _silenceTimer?.Stop();
        AppLogger.Log("終了");
    }

    private void BuildUi()
    {
        Text = "LanMicBridge";
        MinimumSize = new Size(720, 560);
        StartPosition = FormStartPosition.CenterScreen;

        var mainLayout = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            RowCount = 3,
            ColumnCount = 1
        };
        mainLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 60));
        mainLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        mainLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 24));
        Controls.Add(mainLayout);

        var modePanel = new Panel { Dock = DockStyle.Fill };
        _radioReceiver = new RadioButton
        {
            Appearance = Appearance.Button,
            Text = "受信（B）",
            AutoSize = false,
            Width = 160,
            Height = 40,
            Location = new Point(20, 10),
            Checked = true
        };
        _radioReceiver.CheckedChanged += ModeChanged;
        _radioSender = new RadioButton
        {
            Appearance = Appearance.Button,
            Text = "送信（A）",
            AutoSize = false,
            Width = 160,
            Height = 40,
            Location = new Point(190, 10)
        };
        _radioSender.CheckedChanged += ModeChanged;
        modePanel.Controls.Add(_radioReceiver);
        modePanel.Controls.Add(_radioSender);
        mainLayout.Controls.Add(modePanel, 0, 0);

        var contentPanel = new Panel { Dock = DockStyle.Fill };
        mainLayout.Controls.Add(contentPanel, 0, 1);

        _receiverPanel = BuildReceiverPanel();
        _senderPanel = BuildSenderPanel();
        contentPanel.Controls.Add(_receiverPanel);
        contentPanel.Controls.Add(_senderPanel);
        _senderPanel.Visible = false;

        _statusStrip = new StatusStrip { Dock = DockStyle.Fill };
        _statusLabel = new ToolStripStatusLabel { Text = "待受中" };
        _statusStrip.Items.Add(_statusLabel);
        mainLayout.Controls.Add(_statusStrip, 0, 2);

        _statsTimer = new System.Windows.Forms.Timer { Interval = 1000 };
        _statsTimer.Tick += (_, _) => UpdateStatsLabel();
        _receiverStatusTimer = new System.Windows.Forms.Timer { Interval = 500 };
        _receiverStatusTimer.Tick += (_, _) => UpdateReceiverStatus();
        _silenceTimer = new System.Windows.Forms.Timer { Interval = 100 };
        _silenceTimer.Tick += (_, _) => EnsureSilence();

        Load += Form1_Load;
        FormClosing += Form1_FormClosing;
    }
    private Panel BuildReceiverPanel()
    {
        var panel = new Panel { Dock = DockStyle.Fill };
        var layout = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 2,
            RowCount = 13,
            Padding = new Padding(16)
        };
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 180));
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 70));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 30));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 36));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 50));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 28));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 150));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 28));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 40));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 28));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 40));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 40));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 120));
        panel.Controls.Add(layout);

        layout.Controls.Add(new Label { Text = "このPCのIP (IPv4)", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 0);
        var ipPanel = new Panel { Dock = DockStyle.Fill };
        _lblIpList = new Label
        {
            AutoSize = false,
            BorderStyle = BorderStyle.FixedSingle,
            Dock = DockStyle.Fill,
            TextAlign = ContentAlignment.MiddleLeft
        };
        _btnCopyIp = new Button
        {
            Text = "コピー",
            Width = 80,
            Height = 26,
            Anchor = AnchorStyles.Right | AnchorStyles.Top
        };
        _btnCopyIp.Click += (_, _) => CopyIpToClipboard();
        ipPanel.Controls.Add(_lblIpList);
        ipPanel.Controls.Add(_btnCopyIp);
        _btnCopyIp.Location = new Point(ipPanel.Width - _btnCopyIp.Width - 4, 4);
        ipPanel.Resize += (_, _) => _btnCopyIp.Location = new Point(ipPanel.Width - _btnCopyIp.Width - 4, 4);
        layout.Controls.Add(ipPanel, 1, 0);

        layout.Controls.Add(new Label { Text = "待受ポート", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 1);
        _lblPort = new Label { Text = DefaultPort.ToString(), AutoSize = true, Anchor = AnchorStyles.Left };
        layout.Controls.Add(_lblPort, 1, 1);

        layout.Controls.Add(new Label { Text = "VB-CABLE 状態", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 2);
        _lblCableStatus = new Label { Text = "確認中...", AutoSize = true, Anchor = AnchorStyles.Left };
        layout.Controls.Add(_lblCableStatus, 1, 2);

        _lblCableGuide = new Label
        {
            Text = "",
            AutoSize = false,
            Dock = DockStyle.Fill
        };
        layout.SetColumnSpan(_lblCableGuide, 2);
        layout.Controls.Add(_lblCableGuide, 0, 3);

        var meterGroup = new GroupBox { Text = "入力レベルメーター", Dock = DockStyle.Fill };
        var meterLayout = new TableLayoutPanel { Dock = DockStyle.Fill, ColumnCount = 2, RowCount = 4 };
        meterLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 160));
        meterLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        meterLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 30));
        meterLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 30));
        meterLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 30));
        meterLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 30));
        meterGroup.Controls.Add(meterLayout);

        meterLayout.Controls.Add(new Label { Text = "Meter-A (出力前)", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 0);
        _progressMeterA = new ProgressBar { Dock = DockStyle.Fill, Maximum = 100 };
        meterLayout.Controls.Add(_progressMeterA, 1, 0);
        _lblMeterA = new Label { Text = "Peak -∞ dBFS / RMS -∞ dBFS", AutoSize = true, Anchor = AnchorStyles.Left };
        meterLayout.Controls.Add(_lblMeterA, 1, 1);
        meterLayout.Controls.Add(new Label { Text = "Meter-B (ループバック)", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 2);
        _progressMeterB = new ProgressBar { Dock = DockStyle.Fill, Maximum = 100 };
        meterLayout.Controls.Add(_progressMeterB, 1, 2);
        _lblMeterB = new Label { Text = "Peak -∞ dBFS / RMS -∞ dBFS", AutoSize = true, Anchor = AnchorStyles.Left };
        meterLayout.Controls.Add(_lblMeterB, 1, 3);

        _lblMeterWarning = new Label { Text = "", AutoSize = true, Anchor = AnchorStyles.Left };
        layout.Controls.Add(_lblMeterWarning, 0, 4);
        layout.SetColumnSpan(_lblMeterWarning, 2);

        layout.Controls.Add(meterGroup, 0, 5);
        layout.SetColumnSpan(meterGroup, 2);

        _lblMeterGuide = new Label
        {
            Text = "Meter-Aが0付近: 送信停止 / IP / Firewallを確認\nMeter-Aが動くのに拾えない: Aqua Voice入力をCABLE Outputに設定",
            AutoSize = true,
            Anchor = AnchorStyles.Left
        };
        layout.Controls.Add(_lblMeterGuide, 0, 6);
        layout.SetColumnSpan(_lblMeterGuide, 2);

        _lblOutputLevel = new Label { Text = "出力後レベル: Peak -∞ dBFS / RMS -∞ dBFS", AutoSize = true, Anchor = AnchorStyles.Left };
        layout.Controls.Add(_lblOutputLevel, 0, 7);
        layout.SetColumnSpan(_lblOutputLevel, 2);

        _lblStats = new Label { Text = "Packets: 0  Loss: 0%  Jitter: 0ms  Delay: 0ms", AutoSize = true, Anchor = AnchorStyles.Left };
        layout.Controls.Add(_lblStats, 0, 8);
        layout.SetColumnSpan(_lblStats, 2);

        _btnCheckTone = new Button { Text = "チェック音を鳴らす", Width = 160, Height = 30, Anchor = AnchorStyles.Left };
        _btnCheckTone.Click += BtnCheckTone_Click;
        _lblCheckResult = new Label { Text = "結果: -", AutoSize = true, Anchor = AnchorStyles.Left };
        layout.Controls.Add(_btnCheckTone, 0, 9);
        layout.Controls.Add(_lblCheckResult, 1, 9);

        _lblAquaGuide = new Label
        {
            Text = "Aqua Voice > Settings > mic input の Test を押すとループバック確認できます",
            AutoSize = true,
            Anchor = AnchorStyles.Left
        };
        layout.Controls.Add(_lblAquaGuide, 0, 10);
        layout.SetColumnSpan(_lblAquaGuide, 2);

        _linkReceiverDetail = new LinkLabel { Text = "詳細設定を表示", AutoSize = true, Anchor = AnchorStyles.Left };
        _linkReceiverDetail.Click += (_, _) => ToggleReceiverDetail();
        layout.Controls.Add(_linkReceiverDetail, 0, 11);
        layout.SetColumnSpan(_linkReceiverDetail, 2);

        _groupReceiverDetail = new GroupBox { Text = "詳細設定", Dock = DockStyle.Top, Visible = false, Height = 150 };
        var detailLayout = new TableLayoutPanel { Dock = DockStyle.Fill, ColumnCount = 2, RowCount = 4, Padding = new Padding(8) };
        detailLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 160));
        detailLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        _groupReceiverDetail.Controls.Add(detailLayout);
        detailLayout.Controls.Add(new Label { Text = "出力デバイス", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 0);
        _comboOutputDevice = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        _comboOutputDevice.SelectedIndexChanged += (_, _) => ApplyOutputDeviceSelection();
        detailLayout.Controls.Add(_comboOutputDevice, 1, 0);
        detailLayout.Controls.Add(new Label { Text = "ジッタバッファ", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 1);
        _comboJitter = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        _comboJitter.Items.AddRange(new object[] { "Low latency", "Stable" });
        _comboJitter.SelectedIndex = 0;
        _comboJitter.SelectedIndexChanged += (_, _) => RestartOutputForJitter();
        detailLayout.Controls.Add(_comboJitter, 1, 1);
        detailLayout.Controls.Add(new Label { Text = "出力ゲイン", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 2);
        var gainPanel = new Panel { Dock = DockStyle.Fill };
        _trackOutputGain = new TrackBar { Minimum = 25, Maximum = 400, Value = 100, TickFrequency = 25, Dock = DockStyle.Fill };
        _trackOutputGain.Scroll += (_, _) => UpdateOutputGain();
        _lblOutputGainValue = new Label { Text = "100%", AutoSize = true, Dock = DockStyle.Right };
        gainPanel.Controls.Add(_trackOutputGain);
        gainPanel.Controls.Add(_lblOutputGainValue);
        detailLayout.Controls.Add(gainPanel, 1, 2);
        UpdateOutputGain();

        layout.Controls.Add(_groupReceiverDetail, 0, 12);
        layout.SetColumnSpan(_groupReceiverDetail, 2);

        return panel;
    }

    private Panel BuildSenderPanel()
    {
        var panel = new Panel { Dock = DockStyle.Fill };
        var layout = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 2,
            RowCount = 7,
            Padding = new Padding(16)
        };
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 180));
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 40));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 40));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 28));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 30));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 120));
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        layout.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        panel.Controls.Add(layout);

        layout.Controls.Add(new Label { Text = "受信側IPアドレス", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 0);
        _txtIp = new TextBox { Dock = DockStyle.Fill };
        layout.Controls.Add(_txtIp, 1, 0);

        _btnSenderToggle = new Button { Text = "開始", Width = 120, Height = 30, Anchor = AnchorStyles.Left };
        _btnSenderToggle.Click += BtnSenderToggle_Click;
        _lblSenderStatus = new Label { Text = "待機中", AutoSize = true, Anchor = AnchorStyles.Left };
        layout.Controls.Add(_btnSenderToggle, 0, 1);
        layout.Controls.Add(_lblSenderStatus, 1, 1);

        _lblSenderMeter = new Label { Text = "送信入力レベル: Peak -∞ dBFS / RMS -∞ dBFS", AutoSize = true, Anchor = AnchorStyles.Left };
        layout.Controls.Add(_lblSenderMeter, 0, 2);
        layout.SetColumnSpan(_lblSenderMeter, 2);

        _linkSenderDetail = new LinkLabel { Text = "詳細設定を表示", AutoSize = true, Anchor = AnchorStyles.Left };
        _linkSenderDetail.Click += (_, _) => ToggleSenderDetail();
        layout.Controls.Add(_linkSenderDetail, 0, 3);
        layout.SetColumnSpan(_linkSenderDetail, 2);

        _groupSenderDetail = new GroupBox { Text = "詳細設定", Dock = DockStyle.Top, Visible = false, Height = 170 };
        var detailLayout = new TableLayoutPanel { Dock = DockStyle.Fill, ColumnCount = 2, RowCount = 4, Padding = new Padding(8) };
        detailLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 160));
        detailLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        _groupSenderDetail.Controls.Add(detailLayout);
        detailLayout.Controls.Add(new Label { Text = "マイクデバイス", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 0);
        _comboMicDevice = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        detailLayout.Controls.Add(_comboMicDevice, 1, 0);
        detailLayout.Controls.Add(new Label { Text = "送信品質", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 1);
        _comboQuality = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        _comboQuality.Items.AddRange(new object[] { "低", "標準", "高" });
        _comboQuality.SelectedIndex = 1;
        _comboQuality.SelectedIndexChanged += (_, _) => ApplyQualitySelection();
        detailLayout.Controls.Add(_comboQuality, 1, 1);
        detailLayout.Controls.Add(new Label { Text = "送信ゲイン", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 2);
        var gainPanel = new Panel { Dock = DockStyle.Fill };
        _trackGain = new TrackBar { Minimum = 25, Maximum = 200, Value = 100, TickFrequency = 25, Dock = DockStyle.Fill };
        _trackGain.Scroll += (_, _) => UpdateGain();
        _lblGainValue = new Label { Text = "100%", AutoSize = true, Dock = DockStyle.Right };
        gainPanel.Controls.Add(_trackGain);
        gainPanel.Controls.Add(_lblGainValue);
        detailLayout.Controls.Add(gainPanel, 1, 2);
        UpdateGain();
        detailLayout.Controls.Add(new Label { Text = "送信入力レベル", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 3);
        detailLayout.Controls.Add(new Label { Text = "送信入力レベル", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 3);
        _lblSenderMeterDetail = new Label { Text = "Peak -∞ dBFS / RMS -∞ dBFS", AutoSize = true, Anchor = AnchorStyles.Left };
        detailLayout.Controls.Add(_lblSenderMeterDetail, 1, 3);

        layout.Controls.Add(_groupSenderDetail, 0, 4);
        layout.SetColumnSpan(_groupSenderDetail, 2);

        return panel;
    }

    private void ModeChanged(object? sender, EventArgs e)
    {
        if (_radioReceiver.Checked)
        {
            UpdateModeUi(true);
        }
        else if (_radioSender.Checked)
        {
            UpdateModeUi(false);
        }
    }

    private void UpdateModeUi(bool receiverMode)
    {
        _receiverPanel.Visible = receiverMode;
        _senderPanel.Visible = !receiverMode;

        if (receiverMode)
        {
            StopSender();
            if (_receiverUdp == null)
            {
                AppLogger.Init("Receiver");
                StartReceiver();
            }
        }
        else
        {
            StopReceiver();
            AppLogger.Init("Sender");
        }
    }
    private void UpdateIpList()
    {
        var list = GetLocalIpv4Addresses();
        _lblIpList.Text = list.Count == 0 ? "-" : string.Join(Environment.NewLine, list);
    }

    private void CopyIpToClipboard()
    {
        var list = GetLocalIpv4Addresses();
        if (list.Count == 0)
        {
            return;
        }

        Clipboard.SetText(list[0]);
    }

    private static List<string> GetLocalIpv4Addresses()
    {
        var results = new List<(int Score, string Ip)>();
        foreach (var ni in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (ni.OperationalStatus != OperationalStatus.Up)
            {
                continue;
            }

            var props = ni.GetIPProperties();
            foreach (var addr in props.UnicastAddresses)
            {
                if (addr.Address.AddressFamily != AddressFamily.InterNetwork)
                {
                    continue;
                }

                var ip = addr.Address.ToString();
                if (ip.StartsWith("169.254.", StringComparison.Ordinal))
                {
                    continue;
                }

                var score = 0;
                if (ni.NetworkInterfaceType == NetworkInterfaceType.Ethernet)
                {
                    score += 50;
                }
                if (ni.NetworkInterfaceType == NetworkInterfaceType.Wireless80211)
                {
                    score += 40;
                }
                if (!ip.StartsWith("127.", StringComparison.Ordinal))
                {
                    score += 10;
                }
                results.Add((score, ip));
            }
        }

        return results
            .OrderByDescending(item => item.Score)
            .ThenBy(item => item.Ip, StringComparer.Ordinal)
            .Select(item => item.Ip)
            .Distinct(StringComparer.Ordinal)
            .Take(3)
            .ToList();
    }

    private void RefreshDeviceLists()
    {
        _deviceEnumerator?.Dispose();
        _deviceEnumerator = new MMDeviceEnumerator();
        _renderDevices.Clear();
        _captureDevices.Clear();
        _comboOutputDevice.Items.Clear();
        _comboMicDevice.Items.Clear();

        foreach (var device in _deviceEnumerator.EnumerateAudioEndPoints(DataFlow.Render, DeviceState.Active))
        {
            _renderDevices.Add(device);
            _comboOutputDevice.Items.Add(device.FriendlyName);
        }

        foreach (var device in _deviceEnumerator.EnumerateAudioEndPoints(DataFlow.Capture, DeviceState.Active))
        {
            _captureDevices.Add(device);
            _comboMicDevice.Items.Add(device.FriendlyName);
        }

        var recommendedOutput = FindCableInputIndex();
        if (_renderDevices.Count > 0)
        {
            _comboOutputDevice.SelectedIndex = recommendedOutput >= 0 ? recommendedOutput : 0;
        }

        if (_captureDevices.Count > 0)
        {
            _comboMicDevice.SelectedIndex = 0;
        }

        UpdateCableStatus();
    }

    private int FindCableInputIndex()
    {
        for (var i = 0; i < _renderDevices.Count; i++)
        {
            var name = _renderDevices[i].FriendlyName ?? string.Empty;
            if (name.Contains("CABLE Input", StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private int FindCableOutputIndex()
    {
        for (var i = 0; i < _captureDevices.Count; i++)
        {
            var name = _captureDevices[i].FriendlyName ?? string.Empty;
            if (name.Contains("CABLE Output", StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private void UpdateCableStatus()
    {
        var outputIndex = FindCableInputIndex();
        if (outputIndex >= 0)
        {
            _lblCableStatus.Text = "OK: CABLE Input 検出";
            _lblCableGuide.Text = "";
            AppLogger.Log($"VB-CABLE検出 OK Device={_renderDevices[outputIndex].FriendlyName}");
        }
        else
        {
            _lblCableStatus.Text = "NG: CABLE Input が見つかりません";
            _lblCableGuide.Text = "管理者でセットアップを実行し、インストール後に再起動してください。";
            AppLogger.Log("VB-CABLE検出 NG");
        }
    }

    private void ApplyOutputDeviceSelection()
    {
        if (_comboOutputDevice.SelectedIndex < 0 || _comboOutputDevice.SelectedIndex >= _renderDevices.Count)
        {
            return;
        }

        RestartReceiverOutput();
    }

    private void RestartOutputForJitter()
    {
        if (_output != null)
        {
            RestartReceiverOutput();
        }
    }
    private void StartReceiver()
    {
        if (_receiverUdp != null)
        {
            return;
        }

        try
        {
            _decoder = OpusDecoder.Create(SampleRate, Channels);
            _receiverUdp = new UdpClient(DefaultPort);
            _receiverCts = new CancellationTokenSource();
            _receiverTask = Task.Run(() => ReceiveLoop(_receiverCts.Token));
            _statsWatch.Restart();
            _receiverClock.Restart();
            _statsTimer.Start();
            _receiverStatusTimer.Start();
            _silenceTimer.Start();
            RestartReceiverOutput();
            SetStatus("待受中");
            AppLogger.Log("受信待受開始");
        }
        catch (Exception ex)
        {
            AppLogger.LogException("受信開始失敗", ex);
            SetStatus("エラー: 受信開始失敗");
            MessageBox.Show($"受信開始に失敗しました: {ex.Message}", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private void RestartReceiverOutput()
    {
        try
        {
            _output?.Stop();
            _output?.Dispose();
            _output = null;
            _playBuffer = null;

            if (_comboOutputDevice.SelectedIndex < 0 || _comboOutputDevice.SelectedIndex >= _renderDevices.Count)
            {
                return;
            }

            var latency = _comboJitter.SelectedIndex == 1 ? 150 : 50;
            var device = _renderDevices[_comboOutputDevice.SelectedIndex];
            _output = new WasapiOut(device, AudioClientShareMode.Shared, true, latency);
            _playBuffer = new BufferedWaveProvider(new WaveFormat(SampleRate, 16, Channels))
            {
                DiscardOnBufferOverflow = true,
                BufferDuration = TimeSpan.FromSeconds(2)
            };
            _output.Init(_playBuffer);
            _output.Play();
            AppLogger.Log($"出力初期化 Device={device.FriendlyName} Latency={latency}ms");
        }
        catch (Exception ex)
        {
            AppLogger.LogException("出力初期化失敗", ex);
            SetStatus("エラー: 出力初期化失敗");
        }
    }

    private void StopReceiver()
    {
        _receiverCts?.Cancel();
        _receiverTask?.Wait(500);
        _receiverTask = null;
        _receiverCts?.Dispose();
        _receiverCts = null;

        _receiverUdp?.Close();
        _receiverUdp?.Dispose();
        _receiverUdp = null;

        _output?.Stop();
        _output?.Dispose();
        _output = null;
        _playBuffer = null;

        _statsTimer?.Stop();
        _receiverStatusTimer?.Stop();
        _silenceTimer?.Stop();
        _packetsReceived = 0;
        _packetsLost = 0;
        _bytesReceived = 0;
        _hasSequence = false;
    }

    private async Task ReceiveLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested && _receiverUdp != null)
        {
            try
            {
                var result = await _receiverUdp.ReceiveAsync(token);
                ProcessPacket(result.Buffer, result.RemoteEndPoint);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception ex)
            {
                AppLogger.LogException("受信エラー", ex);
                BeginInvoke(() => SetStatus("エラー: 受信エラー"));
            }
        }
    }

    private void ProcessPacket(byte[] buffer, IPEndPoint remote)
    {
        if (!NetworkProtocol.TryParse(buffer, out var type, out var senderId, out var seq, out var payload))
        {
            return;
        }

        _lastPacketTime = DateTime.UtcNow;
        _lastSenderEndpoint = remote;

        switch (type)
        {
            case PacketType.Hello:
                SendAccept(remote, senderId);
                break;
            case PacketType.KeepAlive:
                break;
            case PacketType.Audio:
                HandleAudioPacket(seq, payload);
                break;
        }
    }

    private void SendAccept(IPEndPoint remote, uint senderId)
    {
        if (_receiverUdp == null)
        {
            return;
        }

        var buffer = NetworkProtocol.BuildAccept(senderId);
        _receiverUdp.Send(buffer, buffer.Length, remote);
    }

    private void HandleAudioPacket(uint sequence, ReadOnlySpan<byte> payload)
    {
        _packetsReceived++;
        _bytesReceived += payload.Length;
        UpdateJitter(sequence);

        if (!_hasSequence)
        {
            _expectedSequence = sequence;
            _hasSequence = true;
        }

        if (sequence != _expectedSequence)
        {
            var missing = sequence > _expectedSequence ? (int)(sequence - _expectedSequence) : 0;
            if (missing > 0)
            {
                _packetsLost += missing;
                for (var i = 0; i < missing; i++)
                {
                    DecodeAndPlay(Array.Empty<byte>(), 0, true);
                }
            }
        }

        _expectedSequence = sequence + 1;
        var data = payload.ToArray();
        DecodeAndPlay(data, data.Length, false);
    }

    private void DecodeAndPlay(byte[] payload, int length, bool lost)
    {
        if (_decoder == null || _playBuffer == null)
        {
            return;
        }

        if (DateTime.UtcNow < _suppressNetworkAudioUntil)
        {
            return;
        }

        var pcm = new short[FrameSamples];
        try
        {
            _decoder.Decode(payload, 0, length, pcm, 0, FrameSamples, lost);
        }
        catch
        {
            Array.Clear(pcm, 0, pcm.Length);
        }

        AudioMeter.ComputePeakRms(pcm, out var peak, out var rms);
        _meterA.Update(peak, rms);
        UpdateMeterUi(_meterA, _progressMeterA, _lblMeterA);
        UpdateMeterWarnings(peak, _meterA.SmoothedRmsDb);

        ApplyGain(pcm, _outputGain);
        AudioMeter.ComputePeakRms(pcm, out var outPeak, out var outRms);
        UpdateOutputLevel(outPeak, outRms);

        var bytes = new byte[pcm.Length * 2];
        Buffer.BlockCopy(pcm, 0, bytes, 0, bytes.Length);
        _playBuffer.AddSamples(bytes, 0, bytes.Length);
    }

    private void UpdateJitter(uint sequence)
    {
        var arrivalMs = _receiverClock.Elapsed.TotalMilliseconds;
        var expectedMs = sequence * FrameMs;
        var transit = arrivalMs - expectedMs;
        var d = transit - _lastTransitMs;
        _lastTransitMs = transit;
        _jitterMs += (Math.Abs(d) - _jitterMs) / 16.0;
    }

    private void UpdateStatsLabel()
    {
        var packets = _packetsReceived;
        var lossPercent = packets + _packetsLost == 0 ? 0 : (int)(_packetsLost * 100.0 / (packets + _packetsLost));
        var jitter = (int)Math.Round(_jitterMs);
        var delay = _playBuffer != null ? (int)_playBuffer.BufferedDuration.TotalMilliseconds : 0;
        var text = $"Packets: {packets}  Loss: {lossPercent}%  Jitter: {jitter}ms  Delay: {delay}ms";
        _lblStats.Text = text;

        if ((DateTime.UtcNow - _lastStatsLogged).TotalSeconds >= 10)
        {
            AppLogger.Log($"受信統計 loss={lossPercent}% jitter={jitter}ms delay={delay}ms");
            _lastStatsLogged = DateTime.UtcNow;
        }

        if (_lastSenderEndpoint != null && _receiverUdp != null)
        {
            var statsPacket = NetworkProtocol.BuildStats(0, lossPercent, jitter);
            _receiverUdp.Send(statsPacket, statsPacket.Length, _lastSenderEndpoint);
        }
    }

    private void UpdateReceiverStatus()
    {
        var now = DateTime.UtcNow;
        if (_lastPacketTime == DateTime.MinValue)
        {
            SetStatus("待受中");
            return;
        }

        var elapsed = now - _lastPacketTime;
        if (elapsed.TotalSeconds <= 1)
        {
            _hadConnection = true;
            SetStatus("接続中");
        }
        else if (elapsed.TotalSeconds <= 5 && _hadConnection)
        {
            SetStatus("再接続中");
        }
        else
        {
            SetStatus("待受中");
        }
    }

    private void EnsureSilence()
    {
        if (_playBuffer == null)
        {
            return;
        }

        if (_playBuffer.BufferedDuration.TotalMilliseconds < 40)
        {
            var bytes = new byte[FrameSamples * 2];
            _playBuffer.AddSamples(bytes, 0, bytes.Length);
        }
    }
    private async void BtnCheckTone_Click(object? sender, EventArgs e)
    {
        if (_playBuffer == null)
        {
            MessageBox.Show("出力デバイスが初期化されていません。", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        _suppressNetworkAudioUntil = DateTime.UtcNow.AddSeconds(1.1);
        var tone = GenerateSinePcm16(1000, 1.0f, -12f);
        _playBuffer.AddSamples(tone, 0, tone.Length);

        var pass = await RunLoopbackCheckAsync();
        _lblCheckResult.Text = pass ? "結果: PASS" : "結果: FAIL";
        AppLogger.Log($"チェック音結果 {(pass ? "PASS" : "FAIL")}");
    }

    private async Task<bool> RunLoopbackCheckAsync()
    {
        var outputIndex = FindCableOutputIndex();
        if (outputIndex < 0 || outputIndex >= _captureDevices.Count)
        {
            _lblMeterB.Text = "N/A";
            return false;
        }

        var device = _captureDevices[outputIndex];
        var tcs = new TaskCompletionSource<bool>();
        var capture = new WasapiCapture(device);
        var maxRmsDb = -120f;
        var warningTimer = new System.Windows.Forms.Timer { Interval = 1100 };

        capture.DataAvailable += (_, args) =>
        {
            ReadOnlySpan<byte> buffer = args.Buffer.AsSpan(0, args.BytesRecorded);
            ConvertToPcm16(buffer, capture.WaveFormat, out var pcm);
            AudioMeter.ComputePeakRms(pcm, out var peak, out var rms);
            _meterB.Update(peak, rms);
            UpdateMeterUi(_meterB, _progressMeterB, _lblMeterB);
            var rmsDb = _meterB.SmoothedRmsDb;
            if (rmsDb > maxRmsDb)
            {
                maxRmsDb = rmsDb;
            }
        };

        capture.RecordingStopped += (_, _) =>
        {
            warningTimer.Stop();
            warningTimer.Dispose();
            capture.Dispose();
            var pass = maxRmsDb >= VadThresholdDb;
            tcs.TrySetResult(pass);
        };

        warningTimer.Tick += (_, _) => capture.StopRecording();
        warningTimer.Start();
        capture.StartRecording();

        return await tcs.Task;
    }

    private void ConvertToPcm16(ReadOnlySpan<byte> buffer, WaveFormat format, out short[] pcm)
    {
        if (format.Encoding == WaveFormatEncoding.IeeeFloat && format.BitsPerSample == 32)
        {
            var samples = buffer.Length / 4;
            pcm = new short[samples];
            for (var i = 0; i < samples; i++)
            {
                var value = BitConverter.ToSingle(buffer.Slice(i * 4, 4));
                var clamped = Math.Clamp(value, -1f, 1f);
                pcm[i] = (short)(clamped * short.MaxValue);
            }
            return;
        }

        if (format.BitsPerSample == 16)
        {
            pcm = new short[buffer.Length / 2];
            Buffer.BlockCopy(buffer.ToArray(), 0, pcm, 0, buffer.Length);
            return;
        }

        pcm = Array.Empty<short>();
    }

    private static byte[] GenerateSinePcm16(int frequency, float durationSeconds, float levelDb)
    {
        var samples = (int)(SampleRate * durationSeconds);
        var buffer = new short[samples * Channels];
        var amplitude = (float)Math.Pow(10, levelDb / 20.0);
        var twoPi = 2.0 * Math.PI * frequency;
        var fadeSamples = (int)(SampleRate * 0.01);

        for (var i = 0; i < samples; i++)
        {
            var t = i / (double)SampleRate;
            var value = Math.Sin(twoPi * t) * amplitude;
            if (i < fadeSamples)
            {
                value *= i / (double)fadeSamples;
            }
            else if (i > samples - fadeSamples)
            {
                value *= (samples - i) / (double)fadeSamples;
            }

            var sample = (short)(Math.Clamp(value, -1.0, 1.0) * short.MaxValue);
            buffer[i] = sample;
        }

        var bytes = new byte[buffer.Length * 2];
        Buffer.BlockCopy(buffer, 0, bytes, 0, bytes.Length);
        return bytes;
    }

    private void UpdateMeterUi(AudioMeter meter, ProgressBar progress, Label label)
    {
        var peakDb = meter.SmoothedPeakDb;
        var rmsDb = meter.SmoothedRmsDb;
        var progressValue = DbToProgress(rmsDb);
        if (progress.InvokeRequired)
        {
            progress.BeginInvoke(() =>
            {
                progress.Value = progressValue;
                label.Text = $"Peak {peakDb:0.0} dBFS / RMS {rmsDb:0.0} dBFS";
            });
        }
        else
        {
            progress.Value = progressValue;
            label.Text = $"Peak {peakDb:0.0} dBFS / RMS {rmsDb:0.0} dBFS";
        }
    }

    private void UpdateOutputLevel(float peak, float rms)
    {
        var peakDb = 20f * (float)Math.Log10(peak + 1e-9f);
        var rmsDb = 20f * (float)Math.Log10(rms + 1e-9f);
        var text = $"Peak {peakDb:0.0} dBFS / RMS {rmsDb:0.0} dBFS";
        if (_lblOutputLevel.InvokeRequired)
        {
            _lblOutputLevel.BeginInvoke(() => _lblOutputLevel.Text = text);
        }
        else
        {
            _lblOutputLevel.Text = text;
        }
    }

    private void UpdateSenderMeterText(string text)
    {
        var mainText = $"送信入力レベル: {text}";
        if (_lblSenderMeter.InvokeRequired)
        {
            _lblSenderMeter.BeginInvoke(() => _lblSenderMeter.Text = mainText);
        }
        else
        {
            _lblSenderMeter.Text = mainText;
        }

        if (_lblSenderMeterDetail.InvokeRequired)
        {
            _lblSenderMeterDetail.BeginInvoke(() => _lblSenderMeterDetail.Text = text);
        }
        else
        {
            _lblSenderMeterDetail.Text = text;
        }
    }

    private void UpdateMeterWarnings(float peak, float rmsDb)
    {
        var warnings = new List<string>();
        if (rmsDb < VadThresholdDb)
        {
            _lowRmsSince ??= DateTime.UtcNow;
            if ((DateTime.UtcNow - _lowRmsSince.Value).TotalSeconds >= 2)
            {
                warnings.Add("入力不足: RMSが低い可能性");
            }
        }
        else
        {
            _lowRmsSince = null;
        }
        if (peak > ClipThresholdLinear)
        {
            warnings.Add("クリップ注意");
        }

        var text = warnings.Count == 0 ? "" : string.Join(" / ", warnings);
        if (_lblMeterWarning.InvokeRequired)
        {
            _lblMeterWarning.BeginInvoke(() => _lblMeterWarning.Text = text);
        }
        else
        {
            _lblMeterWarning.Text = text;
        }
    }

    private static int DbToProgress(float db)
    {
        var clamped = Math.Clamp(db, -60f, 0f);
        return (int)Math.Round((clamped + 60f) / 60f * 100f);
    }

    private void BtnSenderToggle_Click(object? sender, EventArgs e)
    {
        if (_senderUdp == null)
        {
            StartSender();
        }
        else
        {
            StopSender();
        }
    }

    private void ApplyQualitySelection()
    {
        switch (_comboQuality.SelectedIndex)
        {
            case 0:
                _selectedBitrate = 16000;
                _selectedComplexity = 3;
                break;
            case 2:
                _selectedBitrate = 64000;
                _selectedComplexity = 8;
                break;
            default:
                _selectedBitrate = 32000;
                _selectedComplexity = 5;
                break;
        }

        if (_encoder != null)
        {
            _encoder.Bitrate = _selectedBitrate;
            _encoder.Complexity = _selectedComplexity;
        }
    }

    private void UpdateGain()
    {
        _sendGain = _trackGain.Value / 100f;
        if (_lblGainValue.InvokeRequired)
        {
            _lblGainValue.BeginInvoke(() => _lblGainValue.Text = $"{_trackGain.Value}%");
        }
        else
        {
            _lblGainValue.Text = $"{_trackGain.Value}%";
        }
    }

    private void UpdateOutputGain()
    {
        _outputGain = _trackOutputGain.Value / 100f;
        if (_lblOutputGainValue.InvokeRequired)
        {
            _lblOutputGainValue.BeginInvoke(() => _lblOutputGainValue.Text = $"{_trackOutputGain.Value}%");
        }
        else
        {
            _lblOutputGainValue.Text = $"{_trackOutputGain.Value}%";
        }
    }

    private static void ApplyGain(short[] pcm, float gain)
    {
        if (Math.Abs(gain - 1f) < 0.001f)
        {
            return;
        }

        for (var i = 0; i < pcm.Length; i++)
        {
            var value = (int)Math.Round(pcm[i] * gain);
            if (value > short.MaxValue)
            {
                value = short.MaxValue;
            }
            else if (value < short.MinValue)
            {
                value = short.MinValue;
            }

            pcm[i] = (short)value;
        }
    }
    private void StartSender()
    {
        if (!IPAddress.TryParse(_txtIp.Text.Trim(), out var ipAddress))
        {
            MessageBox.Show("IPアドレスが正しくありません。", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            return;
        }

        if (_comboMicDevice.SelectedIndex < 0 || _comboMicDevice.SelectedIndex >= _captureDevices.Count)
        {
            MessageBox.Show("マイクデバイスを選択してください。", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            return;
        }

        try
        {
            var device = _captureDevices[_comboMicDevice.SelectedIndex];
            _capture = new WasapiCapture(device, true, FrameMs);
            _captureBuffer = new BufferedWaveProvider(_capture.WaveFormat)
            {
                DiscardOnBufferOverflow = true
            };
            _capture.DataAvailable += CaptureOnDataAvailable;
            _capture.RecordingStopped += CaptureOnRecordingStopped;

            var targetFormat = new WaveFormat(SampleRate, 16, Channels);
            _resampler = new MediaFoundationResampler(_captureBuffer, targetFormat)
            {
                ResamplerQuality = 30
            };

            _encoder = OpusEncoder.Create(SampleRate, Channels, OpusApplication.OPUS_APPLICATION_VOIP);
            _encoder.Bitrate = _selectedBitrate;
            _encoder.Complexity = _selectedComplexity;
            _encoder.SignalType = OpusSignal.OPUS_SIGNAL_VOICE;
            _encoder.UseVBR = true;
            _encoder.UseInbandFEC = true;
            _encoder.PacketLossPercent = 10;

            _senderUdp = new UdpClient(0);
            _senderUdp.Connect(ipAddress, DefaultPort);
            _senderCts = new CancellationTokenSource();
            _senderTask = Task.Run(() => SendLoop(_senderCts.Token));
            _senderReceiveTask = Task.Run(() => SenderReceiveLoop(_senderCts.Token));

            _capture.StartRecording();
            _btnSenderToggle.Text = "停止";
            SetSenderStatus("接続中");
            AppLogger.Log($"送信開始 IP={ipAddress}");
        }
        catch (Exception ex)
        {
            AppLogger.LogException("送信開始失敗", ex);
            StopSender();
            MessageBox.Show($"送信開始に失敗しました: {ex.Message}", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private void StopSender()
    {
        _senderCts?.Cancel();
        _senderTask?.Wait(500);
        _senderReceiveTask?.Wait(500);
        _senderTask = null;
        _senderReceiveTask = null;
        _senderCts?.Dispose();
        _senderCts = null;

        if (_capture != null)
        {
            _capture.DataAvailable -= CaptureOnDataAvailable;
            _capture.RecordingStopped -= CaptureOnRecordingStopped;
            _capture.StopRecording();
            _capture.Dispose();
            _capture = null;
        }

        _resampler?.Dispose();
        _resampler = null;
        _captureBuffer = null;
        _encoder = null;

        _senderUdp?.Close();
        _senderUdp?.Dispose();
        _senderUdp = null;

        _btnSenderToggle.Text = "開始";
        SetSenderStatus("待機中");
        _accepted = false;
    }

    private void CaptureOnRecordingStopped(object? sender, StoppedEventArgs e)
    {
        if (e.Exception != null)
        {
            AppLogger.LogException("録音停止", e.Exception);
            BeginInvoke(() => MessageBox.Show($"録音停止: {e.Exception.Message}", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Error));
            StopSender();
        }
    }

    private void CaptureOnDataAvailable(object? sender, WaveInEventArgs e)
    {
        _captureBuffer?.AddSamples(e.Buffer, 0, e.BytesRecorded);
    }

    private async Task SendLoop(CancellationToken token)
    {
        var frameBytes = FrameSamples * 2;
        var buffer = new byte[frameBytes];
        var pcm = new short[FrameSamples];
        var payload = new byte[4000];

        while (!token.IsCancellationRequested)
        {
            try
            {
                if (_resampler == null || _encoder == null || _senderUdp == null)
                {
                    await Task.Delay(20, token);
                    continue;
                }

                var read = _resampler.Read(buffer, 0, buffer.Length);
                if (read == 0)
                {
                    await Task.Delay(5, token);
                    continue;
                }

                Buffer.BlockCopy(buffer, 0, pcm, 0, frameBytes);
                ApplyGain(pcm, _sendGain);
                AudioMeter.ComputePeakRms(pcm, out var peak, out var rms);
                var rmsDb = 20f * (float)Math.Log10(rms + 1e-9f);
                var now = DateTime.UtcNow;

                if ((now - _lastSenderMeterUpdate).TotalMilliseconds >= 200)
                {
                    var peakDb = 20f * (float)Math.Log10(peak + 1e-9f);
                    var senderText = $"Peak {peakDb:0.0} dBFS / RMS {rmsDb:0.0} dBFS";
                    UpdateSenderMeterText(senderText);

                    _lastSenderMeterUpdate = now;
                }

                if (rmsDb >= VadThresholdDb)
                {
                    _lastVoiceTime = now;
                }

                var withinHangover = (now - _lastVoiceTime).TotalMilliseconds <= VadHangoverMs;
                if (rmsDb >= VadThresholdDb || withinHangover)
                {
                    var encoded = _encoder.Encode(pcm, 0, FrameSamples, payload, 0, payload.Length);
                    var packet = NetworkProtocol.BuildAudio(_senderId, _sendSequence++, payload.AsSpan(0, encoded));
                    _senderUdp.Send(packet, packet.Length);
                    SetSenderStatus(_accepted ? "接続中" : "再接続中");
                }
                else
                {
                    if ((now - _lastKeepAlive).TotalMilliseconds >= KeepAliveMs)
                    {
                        var keep = NetworkProtocol.BuildKeepAlive(_senderId);
                        _senderUdp.Send(keep, keep.Length);
                        _lastKeepAlive = now;
                    }
                }

                if ((now - _lastHello).TotalMilliseconds >= 2000 && !_accepted)
                {
                    var hello = NetworkProtocol.BuildHello(_senderId);
                    _senderUdp.Send(hello, hello.Length);
                    _lastHello = now;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                AppLogger.LogException("送信ループ", ex);
                SetSenderStatus("エラー");
                await Task.Delay(200, token);
            }
        }
    }

    private async Task SenderReceiveLoop(CancellationToken token)
    {
        if (_senderUdp == null)
        {
            return;
        }

        while (!token.IsCancellationRequested)
        {
            try
            {
                var result = await _senderUdp.ReceiveAsync(token);
                if (NetworkProtocol.TryParse(result.Buffer, out var type, out var senderId, out _, out _))
                {
                    if (type == PacketType.Accept && senderId == _senderId)
                    {
                        _accepted = true;
                        _lastAccept = DateTime.UtcNow;
                        SetSenderStatus("接続中");
                    }
                }

                if (_accepted && (DateTime.UtcNow - _lastAccept).TotalSeconds > 5)
                {
                    _accepted = false;
                    SetSenderStatus("再接続中");
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception ex)
            {
                AppLogger.LogException("送信受信", ex);
                await Task.Delay(200, token);
            }
        }
    }

    private void SetSenderStatus(string text)
    {
        if (text != _lastSenderStatus)
        {
            if (text == "再接続中")
            {
                _senderReconnectCount++;
                AppLogger.Log($"送信再接続 count={_senderReconnectCount}");
            }
            else if (text == "待機中" && _lastSenderStatus != string.Empty)
            {
                _senderDisconnectCount++;
                AppLogger.Log($"送信切断 count={_senderDisconnectCount}");
            }
            else
            {
                AppLogger.Log($"送信状態 {text}");
            }

            _lastSenderStatus = text;
        }

        if (_lblSenderStatus.InvokeRequired)
        {
            _lblSenderStatus.BeginInvoke(() => _lblSenderStatus.Text = text);
        }
        else
        {
            _lblSenderStatus.Text = text;
        }
    }

    private void ToggleReceiverDetail()
    {
        _groupReceiverDetail.Visible = !_groupReceiverDetail.Visible;
        _linkReceiverDetail.Text = _groupReceiverDetail.Visible ? "詳細設定を隠す" : "詳細設定を表示";
    }

    private void ToggleSenderDetail()
    {
        _groupSenderDetail.Visible = !_groupSenderDetail.Visible;
        _linkSenderDetail.Text = _groupSenderDetail.Visible ? "詳細設定を隠す" : "詳細設定を表示";
    }

    private void SetStatus(string text)
    {
        if (text != _lastReceiverStatus)
        {
            _lastReceiverStatus = text;
            AppLogger.Log($"受信状態 {text}");
        }

        if (InvokeRequired)
        {
            BeginInvoke(() => _statusLabel.Text = text);
        }
        else
        {
            _statusLabel.Text = text;
        }
    }
}
