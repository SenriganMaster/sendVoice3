namespace LanMicBridge;

internal sealed class SettingsForm : Form
{
    private readonly TabControl _tabs = new();
    private readonly TabPage _receiverTab = new() { Text = "受信", AutoScroll = true };
    private readonly TabPage _senderTab = new() { Text = "送信", AutoScroll = true };
    private readonly TabPage _infoTab = new() { Text = "情報", AutoScroll = true };

    public SettingsForm(Control receiverContent, Control senderContent, Control infoContent)
    {
        Text = "LanMicBridge 設定";
        StartPosition = FormStartPosition.CenterParent;
        MinimumSize = new Size(720, 560);
        Size = new Size(780, 640);

        _tabs.Dock = DockStyle.Fill;
        _tabs.TabPages.Add(_receiverTab);
        _tabs.TabPages.Add(_senderTab);
        _tabs.TabPages.Add(_infoTab);
        Controls.Add(_tabs);

        AttachContent(_receiverTab, receiverContent);
        AttachContent(_senderTab, senderContent);
        AttachContent(_infoTab, infoContent);
    }

    public int SelectedTabIndex
    {
        get => _tabs.SelectedIndex;
        set
        {
            if (value >= 0 && value < _tabs.TabCount)
            {
                _tabs.SelectedIndex = value;
            }
        }
    }

    private static void AttachContent(TabPage page, Control content)
    {
        if (content.Parent != null)
        {
            content.Parent.Controls.Remove(content);
        }

        content.Dock = DockStyle.Top;
        page.Controls.Add(content);
    }
}
