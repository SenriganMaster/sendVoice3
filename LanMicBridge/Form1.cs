
using System.Diagnostics;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Concentus.Enums;
using Concentus.Structs;
using NAudio.CoreAudioApi;
using NAudio.Wave;
using NAudio.Wave.SampleProviders;

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
    private const float AgcTargetRmsDb = -20f;
    private const float AgcNoBoostBelowDb = -50f;
    private const float AgcMaxBoostDb = 24f;
    private const float AgcMaxCutDb = -18f;
    private const float AgcAttack = 0.25f;
    private const float AgcRelease = 0.08f;
    private const float NoiseGateFloorDb = -60f;
    private const float NoiseGateRangeDb = 10f;
    private const float SendAgcTargetRmsDb = -24f;
    private const float SendAgcNoBoostBelowDb = -55f;
    private const float SendAgcMaxBoostDb = 12f;
    private const float SendAgcMaxCutDb = -12f;
    private const float SendAgcAttack = 0.12f;
    private const float SendAgcRelease = 0.05f;
    private const float SendNoiseGateFloorDb = -65f;
    private const float SendNoiseGateRangeDb = 8f;
    private const int TestToneHz = 1000;
    private const float TestToneLevelDb = -12f;
    private const float OutputGainBasePercent = 42f;
    private const float SendGainBasePercent = 40f;
    private const int OutputForceStartMs = 1000;

    private readonly AudioMeter _meterA = new();
    private readonly AudioMeter _meterB = new();
    private readonly Stopwatch _statsWatch = new();
    private readonly Stopwatch _receiverClock = new();

    private MMDeviceEnumerator? _deviceEnumerator;
    private readonly List<MMDevice> _renderDevices = new();
    private readonly List<MMDevice> _captureDevices = new();
    private readonly List<WaveInCapabilities> _mmeDevices = new();

    private WasapiOut? _output;
    private BufferedWaveProvider? _playBuffer;
    private UdpClient? _receiverUdp;
    private CancellationTokenSource? _receiverCts;
    private Task? _receiverTask;
    private OpusDecoder? _decoder;
    private readonly object _jitterLock = new();
    private readonly SortedDictionary<uint, PacketEntry> _jitterBuffer = new();
    private CancellationTokenSource? _playoutCts;
    private Task? _playoutTask;
    private uint _playoutSequence;
    private bool _playoutInitialized;
    private bool _playoutBuffering = true;
    private DateTime _playoutBufferingSince = DateTime.MinValue;
    private int _baseJitterFrames;
    private int _adaptiveJitterFrames;
    private int _minJitterFrames;
    private int _maxJitterFrames;
    private int _jitterWindowFrames;
    private int _jitterMissesWindow;
    private PacketType _lastPacketType = PacketType.Audio;
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

    private IWaveIn? _capture;
    private BufferedWaveProvider? _captureBuffer;
    private ISampleProvider? _sendSampleProvider;
    private float[]? _sendSampleBuffer;
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
    private float _recvAgcGainDb = 0f;
    private float _sendAgcGainDb = 0f;
    private bool _enableRecvProcessing = true;
    private bool _enableSendProcessing = true;
    private bool _sendTestTone;
    private double _sendTestPhase;
    private bool _outputStarted;
    private int _prebufferMs;
    private int _rebufferThresholdMs;
    private bool _sendPcmDirect;
    private AppSettings _appSettings = new();
    private bool _loadingSettings;
    private DateTime _outputStartPendingSince = DateTime.MinValue;

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
    private CheckBox _chkRecvProcessing = null!;

    private TextBox _txtIp = null!;
    private Button _btnSenderToggle = null!;
    private Label _lblSenderStatus = null!;
    private LinkLabel _linkSenderDetail = null!;
    private GroupBox _groupSenderDetail = null!;
    private ComboBox _comboCaptureApi = null!;
    private ComboBox _comboMicDevice = null!;
    private ComboBox _comboQuality = null!;
    private TrackBar _trackGain = null!;
    private Label _lblGainValue = null!;
    private Label _lblSenderMeter = null!;
    private Label _lblSenderMeterDetail = null!;
    private CheckBox _chkSendProcessing = null!;
    private CheckBox _chkSendTestTone = null!;
    private ComboBox _comboSendMode = null!;

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
        LoadAppSettings();
        UpdateModeUi(_radioReceiver.Checked);
    }

    private void Form1_FormClosing(object? sender, FormClosingEventArgs e)
    {
        SaveAppSettings();
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
        var panel = new Panel { Dock = DockStyle.Fill, AutoScroll = true };
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
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 190));
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

        _groupReceiverDetail = new GroupBox { Text = "詳細設定", Dock = DockStyle.Top, Visible = false, Height = 170 };
        var detailLayout = new TableLayoutPanel { Dock = DockStyle.Fill, ColumnCount = 2, RowCount = 6, Padding = new Padding(8) };
        detailLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 160));
        detailLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        _groupReceiverDetail.Controls.Add(detailLayout);
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 40));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        detailLayout.Controls.Add(new Label { Text = "出力デバイス", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 0);
        _comboOutputDevice = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        _comboOutputDevice.SelectedIndexChanged += (_, _) => ApplyOutputDeviceSelection();
        detailLayout.Controls.Add(_comboOutputDevice, 1, 0);
        detailLayout.Controls.Add(new Label { Text = "ジッタバッファ", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 1);
        _comboJitter = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        _comboJitter.Items.AddRange(new object[] { "Low latency", "Stable", "Ultra stable" });
        _comboJitter.SelectedIndex = 0;
        _comboJitter.SelectedIndexChanged += (_, _) => RestartOutputForJitter();
        detailLayout.Controls.Add(_comboJitter, 1, 1);
        detailLayout.Controls.Add(new Label { Text = "出力ゲイン", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 2);
        var gainPanel = new Panel { Dock = DockStyle.Fill };
        _trackOutputGain = new TrackBar { Minimum = 25, Maximum = 1000, Value = 100, TickFrequency = 25, Dock = DockStyle.Fill };
        _trackOutputGain.Scroll += (_, _) => UpdateOutputGain();
        _lblOutputGainValue = new Label { Text = "100%", AutoSize = true, Dock = DockStyle.Right };
        gainPanel.Controls.Add(_trackOutputGain);
        gainPanel.Controls.Add(_lblOutputGainValue);
        detailLayout.Controls.Add(gainPanel, 1, 2);
        UpdateOutputGain();
        detailLayout.Controls.Add(new Label { Text = "音声処理", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 3);
        _chkRecvProcessing = new CheckBox { Text = "AGC/ゲート/クリップ有効", Dock = DockStyle.Fill, Checked = true };
        _chkRecvProcessing.CheckedChanged += (_, _) => _enableRecvProcessing = _chkRecvProcessing.Checked;
        detailLayout.Controls.Add(_chkRecvProcessing, 1, 3);

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
        layout.RowStyles.Add(new RowStyle(SizeType.Absolute, 300));
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

        _groupSenderDetail = new GroupBox { Text = "詳細設定", Dock = DockStyle.Fill, Visible = false, Height = 300 };
        var detailLayout = new TableLayoutPanel { Dock = DockStyle.Fill, ColumnCount = 2, RowCount = 8, Padding = new Padding(8) };
        detailLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 160));
        detailLayout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        _groupSenderDetail.Controls.Add(detailLayout);
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 40));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 26));
        detailLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        detailLayout.Controls.Add(new Label { Text = "入力方式", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 0);
        _comboCaptureApi = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        _comboCaptureApi.Items.AddRange(new object[] { "WASAPI", "MME (互換)" });
        _comboCaptureApi.SelectedIndex = 0;
        _comboCaptureApi.SelectedIndexChanged += (_, _) =>
        {
            StopSender();
            RefreshCaptureDeviceList();
        };
        detailLayout.Controls.Add(_comboCaptureApi, 1, 0);
        detailLayout.Controls.Add(new Label { Text = "マイクデバイス", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 1);
        _comboMicDevice = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        detailLayout.Controls.Add(_comboMicDevice, 1, 1);
        detailLayout.Controls.Add(new Label { Text = "送信品質", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 2);
        _comboQuality = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        _comboQuality.Items.AddRange(new object[] { "低", "標準", "高", "超高" });
        _comboQuality.SelectedIndex = 3;
        _comboQuality.SelectedIndexChanged += (_, _) => ApplyQualitySelection();
        _comboQuality.Enabled = true;
        detailLayout.Controls.Add(_comboQuality, 1, 2);
        ApplyQualitySelection();
        detailLayout.Controls.Add(new Label { Text = "送信方式", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 3);
        _comboSendMode = new ComboBox { Dock = DockStyle.Fill, DropDownStyle = ComboBoxStyle.DropDownList };
        _comboSendMode.Items.AddRange(new object[] { "Opus (推奨)", "PCM直送(テスト)" });
        _comboSendMode.SelectedIndex = 0;
        _comboSendMode.SelectedIndexChanged += (_, _) =>
        {
            _sendPcmDirect = _comboSendMode.SelectedIndex == 1;
            _comboQuality.Enabled = !_sendPcmDirect;
        };
        _sendPcmDirect = _comboSendMode.SelectedIndex == 1;
        detailLayout.Controls.Add(_comboSendMode, 1, 3);
        detailLayout.Controls.Add(new Label { Text = "送信ゲイン", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 4);
        var gainPanel = new Panel { Dock = DockStyle.Fill };
        _trackGain = new TrackBar { Minimum = 25, Maximum = 400, Value = 100, TickFrequency = 25, Dock = DockStyle.Fill };
        _trackGain.Scroll += (_, _) => UpdateGain();
        _lblGainValue = new Label { Text = "100%", AutoSize = true, Dock = DockStyle.Right };
        gainPanel.Controls.Add(_trackGain);
        gainPanel.Controls.Add(_lblGainValue);
        detailLayout.Controls.Add(gainPanel, 1, 4);
        UpdateGain();
        detailLayout.Controls.Add(new Label { Text = "音声処理", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 5);
        _chkSendProcessing = new CheckBox { Text = "AGC/ゲート/クリップ有効", Dock = DockStyle.Fill, Checked = true };
        _chkSendProcessing.CheckedChanged += (_, _) => _enableSendProcessing = _chkSendProcessing.Checked;
        detailLayout.Controls.Add(_chkSendProcessing, 1, 5);
        detailLayout.Controls.Add(new Label { Text = "送信テスト音", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 6);
        _chkSendTestTone = new CheckBox { Text = "1kHzサイン送出", Dock = DockStyle.Fill, Checked = false };
        _chkSendTestTone.CheckedChanged += (_, _) => _sendTestTone = _chkSendTestTone.Checked;
        detailLayout.Controls.Add(_chkSendTestTone, 1, 6);
        detailLayout.Controls.Add(new Label { Text = "送信入力レベル", AutoSize = true, Anchor = AnchorStyles.Left }, 0, 7);
        _lblSenderMeterDetail = new Label { Text = "Peak -∞ dBFS / RMS -∞ dBFS", AutoSize = true, Anchor = AnchorStyles.Left };
        detailLayout.Controls.Add(_lblSenderMeterDetail, 1, 7);

        layout.Controls.Add(_groupSenderDetail, 0, 4);
        layout.SetColumnSpan(_groupSenderDetail, 2);

        return panel;
    }

    private void ModeChanged(object? sender, EventArgs e)
    {
        if (_loadingSettings)
        {
            return;
        }

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

        foreach (var device in _deviceEnumerator.EnumerateAudioEndPoints(DataFlow.Render, DeviceState.Active))
        {
            _renderDevices.Add(device);
            _comboOutputDevice.Items.Add(device.FriendlyName);
        }

        foreach (var device in _deviceEnumerator.EnumerateAudioEndPoints(DataFlow.Capture, DeviceState.Active))
        {
            _captureDevices.Add(device);
        }

        var recommendedOutput = FindCableInputIndex();
        if (_renderDevices.Count > 0)
        {
            _comboOutputDevice.SelectedIndex = recommendedOutput >= 0 ? recommendedOutput : 0;
        }

        RefreshCaptureDeviceList();

        UpdateCableStatus();
    }

    private void RefreshCaptureDeviceList()
    {
        if (_comboMicDevice == null)
        {
            return;
        }

        _comboMicDevice.Items.Clear();
        _mmeDevices.Clear();

        if (_comboCaptureApi.SelectedIndex == 1)
        {
            for (var i = 0; i < WaveInEvent.DeviceCount; i++)
            {
                var caps = WaveInEvent.GetCapabilities(i);
                _mmeDevices.Add(caps);
                _comboMicDevice.Items.Add(caps.ProductName);
            }
        }
        else
        {
            foreach (var device in _captureDevices)
            {
                _comboMicDevice.Items.Add(device.FriendlyName);
            }
        }

        if (_comboMicDevice.Items.Count > 0)
        {
            _comboMicDevice.SelectedIndex = 0;
        }
    }

    private void LoadAppSettings()
    {
        _appSettings = AppSettings.Load();
        ApplySettingsToUi(_appSettings);
    }

    private void SaveAppSettings()
    {
        var settings = CollectAppSettings();
        settings.Save();
        _appSettings = settings;
    }

    private void ApplySettingsToUi(AppSettings settings)
    {
        _loadingSettings = true;
        try
        {
            if (settings.ReceiverDetailVisible.HasValue)
            {
                SetReceiverDetailVisible(settings.ReceiverDetailVisible.Value);
            }

            if (settings.SenderDetailVisible.HasValue)
            {
                SetSenderDetailVisible(settings.SenderDetailVisible.Value);
            }

            var outputIndex = FindRenderDeviceIndex(settings);
            if (outputIndex >= 0 && outputIndex < _comboOutputDevice.Items.Count)
            {
                _comboOutputDevice.SelectedIndex = outputIndex;
            }

            if (settings.JitterIndex is int jitterIndex &&
                jitterIndex >= 0 &&
                jitterIndex < _comboJitter.Items.Count)
            {
                _comboJitter.SelectedIndex = jitterIndex;
            }

            if (settings.OutputGainPercent is int outputGainPercent)
            {
                _trackOutputGain.Value = Math.Clamp(outputGainPercent, _trackOutputGain.Minimum, _trackOutputGain.Maximum);
                UpdateOutputGain();
            }

            if (settings.RecvProcessingEnabled.HasValue)
            {
                _chkRecvProcessing.Checked = settings.RecvProcessingEnabled.Value;
            }

            if (!string.IsNullOrWhiteSpace(settings.SenderIp))
            {
                _txtIp.Text = settings.SenderIp;
            }

            if (settings.CaptureApiIndex is int apiIndex &&
                apiIndex >= 0 &&
                apiIndex < _comboCaptureApi.Items.Count)
            {
                _comboCaptureApi.SelectedIndex = apiIndex;
            }

            var micIndex = FindCaptureDeviceIndex(settings);
            if (micIndex >= 0 && micIndex < _comboMicDevice.Items.Count)
            {
                _comboMicDevice.SelectedIndex = micIndex;
            }

            if (settings.QualityIndex is int qualityIndex &&
                qualityIndex >= 0 &&
                qualityIndex < _comboQuality.Items.Count)
            {
                _comboQuality.SelectedIndex = qualityIndex;
            }
            ApplyQualitySelection();

            if (settings.SendModeIndex is int sendModeIndex &&
                sendModeIndex >= 0 &&
                sendModeIndex < _comboSendMode.Items.Count)
            {
                _comboSendMode.SelectedIndex = sendModeIndex;
            }

            if (settings.SendGainPercent is int sendGainPercent)
            {
                _trackGain.Value = Math.Clamp(sendGainPercent, _trackGain.Minimum, _trackGain.Maximum);
                UpdateGain();
            }

            if (settings.SendProcessingEnabled.HasValue)
            {
                _chkSendProcessing.Checked = settings.SendProcessingEnabled.Value;
            }

            if (settings.SendTestToneEnabled.HasValue)
            {
                _chkSendTestTone.Checked = settings.SendTestToneEnabled.Value;
            }

            var receiverMode = !string.Equals(settings.LastMode, "Sender", StringComparison.OrdinalIgnoreCase);
            _radioReceiver.Checked = receiverMode;
            _radioSender.Checked = !receiverMode;
        }
        finally
        {
            _loadingSettings = false;
        }
    }

    private AppSettings CollectAppSettings()
    {
        var settings = new AppSettings
        {
            LastMode = _radioSender.Checked ? "Sender" : "Receiver",
            JitterIndex = _comboJitter.SelectedIndex,
            OutputGainPercent = _trackOutputGain.Value,
            RecvProcessingEnabled = _chkRecvProcessing.Checked,
            ReceiverDetailVisible = _groupReceiverDetail.Visible,
            SenderIp = _txtIp.Text.Trim(),
            CaptureApiIndex = _comboCaptureApi.SelectedIndex,
            QualityIndex = _comboQuality.SelectedIndex,
            SendModeIndex = _comboSendMode.SelectedIndex,
            SendGainPercent = _trackGain.Value,
            SendProcessingEnabled = _chkSendProcessing.Checked,
            SendTestToneEnabled = _chkSendTestTone.Checked,
            SenderDetailVisible = _groupSenderDetail.Visible
        };

        if (_comboOutputDevice.SelectedIndex >= 0 &&
            _comboOutputDevice.SelectedIndex < _renderDevices.Count)
        {
            var device = _renderDevices[_comboOutputDevice.SelectedIndex];
            settings.OutputDeviceId = device.ID;
            settings.OutputDeviceName = device.FriendlyName;
            settings.OutputDeviceIndex = _comboOutputDevice.SelectedIndex;
        }

        if (_comboCaptureApi.SelectedIndex == 1)
        {
            settings.CaptureMmeIndex = _comboMicDevice.SelectedIndex;
            if (_comboMicDevice.SelectedIndex >= 0 && _comboMicDevice.SelectedIndex < _mmeDevices.Count)
            {
                settings.CaptureDeviceName = _mmeDevices[_comboMicDevice.SelectedIndex].ProductName;
            }
        }
        else
        {
            settings.CaptureDeviceIndex = _comboMicDevice.SelectedIndex;
            if (_comboMicDevice.SelectedIndex >= 0 && _comboMicDevice.SelectedIndex < _captureDevices.Count)
            {
                var device = _captureDevices[_comboMicDevice.SelectedIndex];
                settings.CaptureDeviceId = device.ID;
                settings.CaptureDeviceName = device.FriendlyName;
            }
        }

        return settings;
    }

    private int FindRenderDeviceIndex(AppSettings settings)
    {
        if (!string.IsNullOrWhiteSpace(settings.OutputDeviceId))
        {
            for (var i = 0; i < _renderDevices.Count; i++)
            {
                if (string.Equals(_renderDevices[i].ID, settings.OutputDeviceId, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(settings.OutputDeviceName))
        {
            for (var i = 0; i < _renderDevices.Count; i++)
            {
                if (string.Equals(_renderDevices[i].FriendlyName, settings.OutputDeviceName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
        }

        if (settings.OutputDeviceIndex is int index && index >= 0 && index < _renderDevices.Count)
        {
            return index;
        }

        return -1;
    }

    private int FindCaptureDeviceIndex(AppSettings settings)
    {
        if (_comboCaptureApi.SelectedIndex == 1)
        {
            if (settings.CaptureMmeIndex is int mmeIndex &&
                mmeIndex >= 0 &&
                mmeIndex < _mmeDevices.Count)
            {
                return mmeIndex;
            }

            if (!string.IsNullOrWhiteSpace(settings.CaptureDeviceName))
            {
                for (var i = 0; i < _mmeDevices.Count; i++)
                {
                    if (string.Equals(_mmeDevices[i].ProductName, settings.CaptureDeviceName, StringComparison.OrdinalIgnoreCase))
                    {
                        return i;
                    }
                }
            }

            return -1;
        }

        if (!string.IsNullOrWhiteSpace(settings.CaptureDeviceId))
        {
            for (var i = 0; i < _captureDevices.Count; i++)
            {
                if (string.Equals(_captureDevices[i].ID, settings.CaptureDeviceId, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(settings.CaptureDeviceName))
        {
            for (var i = 0; i < _captureDevices.Count; i++)
            {
                if (string.Equals(_captureDevices[i].FriendlyName, settings.CaptureDeviceName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
        }

        if (settings.CaptureDeviceIndex is int captureIndex &&
            captureIndex >= 0 &&
            captureIndex < _captureDevices.Count)
        {
            return captureIndex;
        }

        return -1;
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
            _receiverUdp.Client.ReceiveBufferSize = 1024 * 1024;
            _receiverCts = new CancellationTokenSource();
            _receiverTask = Task.Run(() => ReceiveLoop(_receiverCts.Token));
            _playoutCts = new CancellationTokenSource();
            _playoutTask = Task.Run(() => PlayoutLoop(_playoutCts.Token));
            _statsWatch.Restart();
            _receiverClock.Restart();
            _statsTimer.Start();
            _receiverStatusTimer.Start();
            _silenceTimer.Start();
            ResetPlayoutState();
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

            var latency = _comboJitter.SelectedIndex switch
            {
                1 => 140,
                2 => 220,
                _ => 60
            };
            var bufferSeconds = _comboJitter.SelectedIndex switch
            {
                1 => 3.5,
                2 => 4.5,
                _ => 1.8
            };
            var device = _renderDevices[_comboOutputDevice.SelectedIndex];
            _output = new WasapiOut(device, AudioClientShareMode.Shared, true, latency);
            _playBuffer = new BufferedWaveProvider(new WaveFormat(SampleRate, 16, Channels))
            {
                DiscardOnBufferOverflow = true,
                BufferDuration = TimeSpan.FromSeconds(bufferSeconds)
            };
            _output.Init(_playBuffer);
            _outputStarted = false;
            _prebufferMs = _comboJitter.SelectedIndex switch
            {
                1 => 220,
                2 => 320,
                _ => 120
            };
            _baseJitterFrames = Math.Max(3, _prebufferMs / FrameMs);
            _adaptiveJitterFrames = _baseJitterFrames;
            _minJitterFrames = Math.Max(3, _baseJitterFrames - 2);
            _maxJitterFrames = _baseJitterFrames + 8;
            _rebufferThresholdMs = Math.Max(40, _prebufferMs / 2);
            AppLogger.Log($"出力初期化 Device={device.FriendlyName} Latency={latency}ms Prebuffer={_prebufferMs}ms JitterFrames={_baseJitterFrames}");
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
        WaitTaskSafely(_receiverTask, 500);
        _receiverTask = null;
        _receiverCts?.Dispose();
        _receiverCts = null;
        _playoutCts?.Cancel();
        WaitTaskSafely(_playoutTask, 500);
        _playoutTask = null;
        _playoutCts?.Dispose();
        _playoutCts = null;

        _receiverUdp?.Close();
        _receiverUdp?.Dispose();
        _receiverUdp = null;

        _output?.Stop();
        _output?.Dispose();
        _output = null;
        _playBuffer = null;
        _outputStarted = false;

        _statsTimer?.Stop();
        _receiverStatusTimer?.Stop();
        _silenceTimer?.Stop();
        _packetsReceived = 0;
        _packetsLost = 0;
        _bytesReceived = 0;
        ResetPlayoutState();
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
            case PacketType.Pcm:
                HandlePcmPacket(seq, payload);
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
        EnqueuePacket(sequence, PacketType.Audio, payload);
    }

    private void HandlePcmPacket(uint sequence, ReadOnlySpan<byte> payload)
    {
        EnqueuePacket(sequence, PacketType.Pcm, payload);
    }

    private void EnqueuePacket(uint sequence, PacketType type, ReadOnlySpan<byte> payload)
    {
        _packetsReceived++;
        _bytesReceived += payload.Length;
        UpdateJitter(sequence);

        var data = payload.ToArray();
        lock (_jitterLock)
        {
            if (!_jitterBuffer.ContainsKey(sequence))
            {
                _jitterBuffer[sequence] = new PacketEntry(type, data);
            }

            _lastPacketType = type;
            if (!_playoutInitialized)
            {
                _playoutSequence = sequence;
                _playoutInitialized = true;
                _playoutBuffering = true;
                _playoutBufferingSince = DateTime.UtcNow;
            }
        }
    }

    private async Task PlayoutLoop(CancellationToken token)
    {
        var frameDuration = TimeSpan.FromMilliseconds(FrameMs);
        var stopwatch = Stopwatch.StartNew();
        var next = stopwatch.Elapsed;

        while (!token.IsCancellationRequested)
        {
            if (!_playoutInitialized)
            {
                await Task.Delay(5, token);
                continue;
            }

            if (_lastPacketTime != DateTime.MinValue && (DateTime.UtcNow - _lastPacketTime).TotalSeconds > 2)
            {
                ResetPlayoutState();
                await Task.Delay(50, token);
                continue;
            }

            if (_playoutBuffering)
            {
                var bufferedCount = GetBufferedCount();
                var bufferedEnough = bufferedCount >= _adaptiveJitterFrames;
                var bufferedTooLong = bufferedCount > 0 && _playoutBufferingSince != DateTime.MinValue &&
                    (DateTime.UtcNow - _playoutBufferingSince).TotalSeconds >= 0.5;
                if (bufferedEnough || bufferedTooLong)
                {
                    _playoutBuffering = false;
                    next = stopwatch.Elapsed;
                    AppLogger.Log($"再生開始 JitterFrames={_adaptiveJitterFrames} Buffered={bufferedCount}");
                }
                else
                {
                    await Task.Delay(5, token);
                    continue;
                }
            }
            else
            {
                var bufferedCount = GetBufferedCount();
                if (bufferedCount > _adaptiveJitterFrames * 3 && TryGetMinSequence(out var minSeq))
                {
                    _playoutSequence = minSeq;
                    AppLogger.Log($"再同期 PlayoutSeq={_playoutSequence} Buffered={bufferedCount}");
                }
            }

            PlaySequence(_playoutSequence);
            _playoutSequence++;

            next += frameDuration;
            var delay = next - stopwatch.Elapsed;
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, token);
            }
        }
    }

    private void PlaySequence(uint sequence)
    {
        var gotPacket = TryDequeuePacket(sequence, out var entry) && entry != null;
        if (gotPacket)
        {
            if (entry.Type == PacketType.Audio)
            {
                DecodeAndPlay(entry.Payload, entry.Payload.Length, false, false);
            }
            else
            {
                ProcessPcmPayload(entry.Payload);
            }

            AdjustJitterTarget(true);
            return;
        }

        _packetsLost++;

        if (TryPeekPacket(sequence + 1, out var next) && next != null && next.Type == PacketType.Audio)
        {
            DecodeAndPlay(next.Payload, next.Payload.Length, false, true);
            return;
        }

        if (_lastPacketType == PacketType.Pcm)
        {
            ProcessPcmAndPlay(new short[FrameSamples]);
        }
        else
        {
            DecodeAndPlay(Array.Empty<byte>(), 0, true, false);
        }

        AdjustJitterTarget(false);
    }

    private void ProcessPcmPayload(byte[] payload)
    {
        var pcm = new short[FrameSamples];
        var frameBytes = FrameSamples * 2;
        if (payload.Length >= frameBytes)
        {
            Buffer.BlockCopy(payload, 0, pcm, 0, frameBytes);
        }
        else if (payload.Length > 0)
        {
            Buffer.BlockCopy(payload, 0, pcm, 0, payload.Length);
        }

        ProcessPcmAndPlay(pcm);
    }

    private bool TryDequeuePacket(uint sequence, out PacketEntry? entry)
    {
        lock (_jitterLock)
        {
            if (_jitterBuffer.TryGetValue(sequence, out entry))
            {
                _jitterBuffer.Remove(sequence);
                return true;
            }
        }

        entry = null;
        return false;
    }

    private bool TryPeekPacket(uint sequence, out PacketEntry? entry)
    {
        lock (_jitterLock)
        {
            if (_jitterBuffer.TryGetValue(sequence, out entry))
            {
                return true;
            }
        }

        entry = null;
        return false;
    }

    private int GetBufferedCount()
    {
        lock (_jitterLock)
        {
            return _jitterBuffer.Count;
        }
    }

    private bool TryGetMinSequence(out uint sequence)
    {
        lock (_jitterLock)
        {
            foreach (var key in _jitterBuffer.Keys)
            {
                sequence = key;
                return true;
            }
        }

        sequence = 0;
        return false;
    }

    private void ResetPlayoutState()
    {
        lock (_jitterLock)
        {
            _jitterBuffer.Clear();
        }

        _playoutInitialized = false;
        _playoutBuffering = true;
        _playoutSequence = 0;
        _lastPacketType = PacketType.Audio;
        _playoutBufferingSince = DateTime.MinValue;
        _adaptiveJitterFrames = _baseJitterFrames;
        _jitterWindowFrames = 0;
        _jitterMissesWindow = 0;
        _outputStartPendingSince = DateTime.MinValue;
    }

    private void AdjustJitterTarget(bool gotPacket)
    {
        _jitterWindowFrames++;
        if (!gotPacket)
        {
            _jitterMissesWindow++;
        }

        if (_jitterWindowFrames < 50)
        {
            return;
        }

        var missRate = _jitterMissesWindow / (double)_jitterWindowFrames;
        var bufferedCount = GetBufferedCount();

        if (missRate > 0.02 && _adaptiveJitterFrames < _maxJitterFrames)
        {
            var step = missRate > 0.1 ? 2 : 1;
            _adaptiveJitterFrames = Math.Min(_adaptiveJitterFrames + step, _maxJitterFrames);
            AppLogger.Log($"Jitter target ↑ {_adaptiveJitterFrames} missRate={missRate:0.0%}");
        }
        else if (missRate == 0 && bufferedCount > _adaptiveJitterFrames + 6 && _adaptiveJitterFrames > _minJitterFrames)
        {
            _adaptiveJitterFrames = Math.Max(_adaptiveJitterFrames - 1, _minJitterFrames);
            AppLogger.Log($"Jitter target ↓ {_adaptiveJitterFrames}");
        }

        _jitterWindowFrames = 0;
        _jitterMissesWindow = 0;
    }

    private void DecodeAndPlay(byte[] payload, int length, bool lost, bool useFec)
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
            if (lost)
            {
                _decoder.Decode(Array.Empty<byte>(), 0, 0, pcm, 0, FrameSamples, false);
            }
            else
            {
                _decoder.Decode(payload, 0, length, pcm, 0, FrameSamples, useFec);
            }
        }
        catch
        {
            Array.Clear(pcm, 0, pcm.Length);
        }

        ProcessPcmAndPlay(pcm);
    }

    private void ProcessPcmAndPlay(short[] pcm)
    {
        if (_playBuffer == null)
        {
            return;
        }

        AudioMeter.ComputePeakRms(pcm, out var peak, out var rms);
        _meterA.Update(peak, rms);
        UpdateMeterUi(_meterA, _progressMeterA, _lblMeterA);
        UpdateMeterWarnings(peak, _meterA.SmoothedRmsDb);

        var rmsDbPre = LinearToDb(rms);
        float outPeak;
        float outRms;
        if (_enableRecvProcessing)
        {
            ApplyAgcAndGain(
                pcm,
                rmsDbPre,
                ref _recvAgcGainDb,
                _outputGain,
                out outPeak,
                out outRms,
                AgcTargetRmsDb,
                AgcNoBoostBelowDb,
                AgcMaxBoostDb,
                AgcMaxCutDb,
                AgcAttack,
                AgcRelease,
                NoiseGateFloorDb,
                NoiseGateRangeDb);
        }
        else
        {
            ApplyGain(pcm, _outputGain);
            AudioMeter.ComputePeakRms(pcm, out outPeak, out outRms);
        }
        UpdateOutputLevel(outPeak, outRms);

        var bytes = new byte[pcm.Length * 2];
        Buffer.BlockCopy(pcm, 0, bytes, 0, bytes.Length);
        _playBuffer.AddSamples(bytes, 0, bytes.Length);
        MaybeStartOutput();
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
        if (_playBuffer == null || _output == null)
        {
            return;
        }

        var bufferedMs = _playBuffer.BufferedDuration.TotalMilliseconds;
        if (_outputStarted && bufferedMs < _rebufferThresholdMs)
        {
            _output.Pause();
            _outputStarted = false;
            _outputStartPendingSince = DateTime.MinValue;
            AppLogger.Log("出力再バッファ開始");
            return;
        }

        if (_outputStarted && bufferedMs < 40)
        {
            var bytes = new byte[FrameSamples * 2];
            _playBuffer.AddSamples(bytes, 0, bytes.Length);
        }
    }

    private void MaybeStartOutput()
    {
        if (_output == null || _playBuffer == null || _outputStarted)
        {
            return;
        }

        var bufferedMs = _playBuffer.BufferedDuration.TotalMilliseconds;
        if (bufferedMs <= 0)
        {
            _outputStartPendingSince = DateTime.MinValue;
            return;
        }

        if (bufferedMs >= _prebufferMs)
        {
            _output.Play();
            _outputStarted = true;
            _outputStartPendingSince = DateTime.MinValue;
            AppLogger.Log($"出力開始 Prebuffer={_prebufferMs}ms");
            return;
        }

        if (_outputStartPendingSince == DateTime.MinValue)
        {
            _outputStartPendingSince = DateTime.UtcNow;
            return;
        }

        if ((DateTime.UtcNow - _outputStartPendingSince).TotalMilliseconds >= OutputForceStartMs)
        {
            _output.Play();
            _outputStarted = true;
            _outputStartPendingSince = DateTime.MinValue;
            AppLogger.Log($"出力強制開始 Buffered={bufferedMs:0}ms");
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

    private sealed class PacketEntry
    {
        public PacketType Type { get; }
        public byte[] Payload { get; }

        public PacketEntry(PacketType type, byte[] payload)
        {
            Type = type;
            Payload = payload;
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
            case 3:
                _selectedBitrate = 128000;
                _selectedComplexity = 10;
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
        _sendGain = (_trackGain.Value / 100f) * (SendGainBasePercent / 100f);
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
        _outputGain = (_trackOutputGain.Value / 100f) * (OutputGainBasePercent / 100f);
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

    private static void ApplyAgcAndGain(
        short[] pcm,
        float rmsDbPre,
        ref float agcGainDb,
        float userGain,
        out float outPeak,
        out float outRms,
        float targetRmsDb,
        float noBoostBelowDb,
        float maxBoostDb,
        float maxCutDb,
        float attack,
        float release,
        float gateFloorDb,
        float gateRangeDb)
    {
        float targetGainDb;
        if (rmsDbPre < noBoostBelowDb)
        {
            targetGainDb = 0f;
        }
        else
        {
            targetGainDb = Math.Clamp(targetRmsDb - rmsDbPre, maxCutDb, maxBoostDb);
        }

        var lerp = targetGainDb > agcGainDb ? attack : release;
        agcGainDb = Lerp(agcGainDb, targetGainDb, lerp);

        var gate = Math.Clamp((rmsDbPre - gateFloorDb) / gateRangeDb, 0f, 1f);
        var gainLinear = DbToLinear(agcGainDb) * userGain * gate;
        ApplyGainWithSoftClip(pcm, gainLinear);

        AudioMeter.ComputePeakRms(pcm, out outPeak, out outRms);
    }

    private static void ApplyGainWithSoftClip(short[] pcm, float gain)
    {
        if (Math.Abs(gain - 1f) < 0.001f)
        {
            return;
        }

        for (var i = 0; i < pcm.Length; i++)
        {
            var x = (pcm[i] / 32768f) * gain;
            var y = (float)Math.Tanh(x);
            var value = (int)Math.Round(y * short.MaxValue);
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

    private static float DbToLinear(float db)
    {
        return (float)Math.Pow(10.0, db / 20.0);
    }

    private static float LinearToDb(float linear)
    {
        return 20f * (float)Math.Log10(linear + 1e-9f);
    }

    private static float Lerp(float a, float b, float t)
    {
        return a + (b - a) * t;
    }

    private void FillTestToneFrame(short[] pcm)
    {
        var amplitude = (float)Math.Pow(10, TestToneLevelDb / 20.0);
        var phaseStep = 2.0 * Math.PI * TestToneHz / SampleRate;

        for (var i = 0; i < pcm.Length; i++)
        {
            var value = Math.Sin(_sendTestPhase) * amplitude;
            _sendTestPhase += phaseStep;
            if (_sendTestPhase >= Math.PI * 2)
            {
                _sendTestPhase -= Math.PI * 2;
            }

            pcm[i] = (short)Math.Round(Math.Clamp(value, -1.0, 1.0) * short.MaxValue);
        }
    }
    private void StartSender()
    {
        if (!IPAddress.TryParse(_txtIp.Text.Trim(), out var ipAddress))
        {
            MessageBox.Show("IPアドレスが正しくありません。", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            return;
        }

        try
        {
            var useMme = _comboCaptureApi.SelectedIndex == 1;
            if (useMme)
            {
                var mmeIndex = _comboMicDevice.SelectedIndex;
                if (mmeIndex < 0 || mmeIndex >= WaveInEvent.DeviceCount)
                {
                    MessageBox.Show("マイクデバイスを選択してください。", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return;
                }

                var waveIn = new WaveInEvent
                {
                    DeviceNumber = mmeIndex,
                    BufferMilliseconds = 50,
                    NumberOfBuffers = 3
                };
                _capture = waveIn;
            }
            else
            {
                if (_comboMicDevice.SelectedIndex < 0 || _comboMicDevice.SelectedIndex >= _captureDevices.Count)
                {
                    MessageBox.Show("マイクデバイスを選択してください。", "LanMicBridge", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return;
                }

                var device = _captureDevices[_comboMicDevice.SelectedIndex];
                _capture = new WasapiCapture(device, true, 50);
            }

            _captureBuffer = new BufferedWaveProvider(_capture.WaveFormat)
            {
                DiscardOnBufferOverflow = true,
                ReadFully = true,
                BufferDuration = TimeSpan.FromSeconds(1)
            };
            _capture.DataAvailable += CaptureOnDataAvailable;
            _capture.RecordingStopped += CaptureOnRecordingStopped;

            var sampleProvider = _captureBuffer.ToSampleProvider();
            var inputChannels = sampleProvider.WaveFormat.Channels;
            if (inputChannels == 2)
            {
                sampleProvider = new StereoToMonoSampleProvider(sampleProvider)
                {
                    LeftVolume = 0.5f,
                    RightVolume = 0.5f
                };
            }
            else if (inputChannels != 1)
            {
                var mux = new MultiplexingSampleProvider(new[] { sampleProvider }, 1);
                mux.ConnectInputToOutput(0, 0);
                sampleProvider = mux;
                AppLogger.Log($"送信入力チャンネル数 {inputChannels} -> 1 に変換");
            }

            if (sampleProvider.WaveFormat.SampleRate != SampleRate)
            {
                sampleProvider = new WdlResamplingSampleProvider(sampleProvider, SampleRate);
            }

            _sendSampleProvider = sampleProvider;
            _sendSampleBuffer = new float[FrameSamples];

            _encoder = OpusEncoder.Create(SampleRate, Channels, OpusApplication.OPUS_APPLICATION_VOIP);
            _encoder.Bitrate = _selectedBitrate;
            _encoder.Complexity = _selectedComplexity;
            _encoder.SignalType = OpusSignal.OPUS_SIGNAL_VOICE;
            _encoder.UseVBR = true;
            _encoder.UseInbandFEC = true;
            _encoder.PacketLossPercent = 20;

            _senderUdp = new UdpClient(0);
            _senderUdp.Client.SendBufferSize = 1024 * 1024;
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
        WaitTaskSafely(_senderTask, 500);
        WaitTaskSafely(_senderReceiveTask, 500);
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

        _sendSampleProvider = null;
        _sendSampleBuffer = null;
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
        var pcm = new short[FrameSamples];
        var payload = new byte[4000];
        var pcmBytes = new byte[FrameSamples * 2];

        while (!token.IsCancellationRequested)
        {
            try
            {
                var senderUdp = _senderUdp;
                if (_sendSampleProvider == null || _sendSampleBuffer == null || senderUdp == null)
                {
                    await Task.Delay(20, token);
                    continue;
                }
                var encoder = _encoder;
                if (!_sendPcmDirect && encoder == null)
                {
                    await Task.Delay(20, token);
                    continue;
                }

                if (_sendTestTone)
                {
                    FillTestToneFrame(pcm);
                }
                else
                {
                    var samplesRead = _sendSampleProvider.Read(_sendSampleBuffer, 0, FrameSamples);
                    if (samplesRead == 0)
                    {
                        Array.Clear(pcm, 0, pcm.Length);
                    }
                    else
                    {
                        if (samplesRead < FrameSamples)
                        {
                            Array.Clear(_sendSampleBuffer, samplesRead, FrameSamples - samplesRead);
                        }

                        for (var i = 0; i < FrameSamples; i++)
                        {
                            var sample = Math.Clamp(_sendSampleBuffer[i], -1f, 1f);
                            pcm[i] = (short)Math.Round(sample * short.MaxValue);
                        }
                    }
                }
                ApplyGain(pcm, _sendGain);
                AudioMeter.ComputePeakRms(pcm, out var peakPre, out var rmsPre);
                var rmsDbPre = LinearToDb(rmsPre);
                var now = DateTime.UtcNow;

                if (rmsDbPre >= VadThresholdDb || _sendTestTone)
                {
                    _lastVoiceTime = now;
                }

                var withinHangover = (now - _lastVoiceTime).TotalMilliseconds <= VadHangoverMs;
                float peak;
                float rms;
                if (_enableSendProcessing)
                {
                    ApplyAgcAndGain(
                        pcm,
                        rmsDbPre,
                        ref _sendAgcGainDb,
                        _sendGain,
                        out peak,
                        out rms,
                        SendAgcTargetRmsDb,
                        SendAgcNoBoostBelowDb,
                        SendAgcMaxBoostDb,
                        SendAgcMaxCutDb,
                        SendAgcAttack,
                        SendAgcRelease,
                        SendNoiseGateFloorDb,
                        SendNoiseGateRangeDb);
                }
                else
                {
                    ApplyGain(pcm, _sendGain);
                    AudioMeter.ComputePeakRms(pcm, out peak, out rms);
                }
                var rmsDbPost = LinearToDb(rms);

                if ((now - _lastSenderMeterUpdate).TotalMilliseconds >= 200)
                {
                    var peakDb = LinearToDb(peak);
                    var senderText = $"Peak {peakDb:0.0} dBFS / RMS {rmsDbPost:0.0} dBFS";
                    UpdateSenderMeterText(senderText);

                    _lastSenderMeterUpdate = now;
                }

                var sentAudio = false;
                if (_sendPcmDirect)
                {
                    Buffer.BlockCopy(pcm, 0, pcmBytes, 0, pcmBytes.Length);
                    var pcmPacket = NetworkProtocol.BuildPcm(_senderId, _sendSequence++, pcmBytes);
                    senderUdp.Send(pcmPacket, pcmPacket.Length);
                    sentAudio = true;
                    SetSenderStatus(_accepted ? "接続中" : "再接続中");
                }
                else if (rmsDbPre >= VadThresholdDb || withinHangover || _sendTestTone)
                {
                    var encoded = encoder!.Encode(pcm, 0, FrameSamples, payload, 0, payload.Length);
                    var packet = NetworkProtocol.BuildAudio(_senderId, _sendSequence++, payload.AsSpan(0, encoded));
                    senderUdp.Send(packet, packet.Length);
                    sentAudio = true;
                    SetSenderStatus(_accepted ? "接続中" : "再接続中");
                }
                else if (!sentAudio)
                {
                    if ((now - _lastKeepAlive).TotalMilliseconds >= KeepAliveMs)
                    {
                        var keep = NetworkProtocol.BuildKeepAlive(_senderId);
                        senderUdp.Send(keep, keep.Length);
                        _lastKeepAlive = now;
                    }
                }

                if ((now - _lastHello).TotalMilliseconds >= 2000 && !_accepted)
                {
                    var hello = NetworkProtocol.BuildHello(_senderId);
                    senderUdp.Send(hello, hello.Length);
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

            await Task.Delay(FrameMs, token);
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
        SetReceiverDetailVisible(!_groupReceiverDetail.Visible);
    }

    private void ToggleSenderDetail()
    {
        SetSenderDetailVisible(!_groupSenderDetail.Visible);
    }

    private void SetReceiverDetailVisible(bool visible)
    {
        _groupReceiverDetail.Visible = visible;
        _linkReceiverDetail.Text = visible ? "詳細設定を隠す" : "詳細設定を表示";
    }

    private void SetSenderDetailVisible(bool visible)
    {
        _groupSenderDetail.Visible = visible;
        _linkSenderDetail.Text = visible ? "詳細設定を隠す" : "詳細設定を表示";
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

    private static void WaitTaskSafely(Task? task, int millisecondsTimeout)
    {
        if (task == null)
        {
            return;
        }

        try
        {
            task.Wait(millisecondsTimeout);
        }
        catch (AggregateException ex)
        {
            ex.Handle(inner => inner is TaskCanceledException);
        }
        catch (TaskCanceledException)
        {
        }
    }
}
