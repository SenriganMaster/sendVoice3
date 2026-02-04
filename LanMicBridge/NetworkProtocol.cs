using System.Buffers.Binary;

namespace LanMicBridge;

internal enum PacketType : byte
{
    Hello = 1,
    Accept = 2,
    Audio = 3,
    KeepAlive = 4,
    Stats = 5
}

internal static class NetworkProtocol
{
    public const byte Version = 1;

    public static byte[] BuildHello(uint senderId)
    {
        var buffer = new byte[6];
        buffer[0] = (byte)PacketType.Hello;
        buffer[1] = Version;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(2, 4), senderId);
        return buffer;
    }

    public static byte[] BuildAccept(uint senderId)
    {
        var buffer = new byte[6];
        buffer[0] = (byte)PacketType.Accept;
        buffer[1] = Version;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(2, 4), senderId);
        return buffer;
    }

    public static byte[] BuildKeepAlive(uint senderId)
    {
        var buffer = new byte[6];
        buffer[0] = (byte)PacketType.KeepAlive;
        buffer[1] = Version;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(2, 4), senderId);
        return buffer;
    }

    public static byte[] BuildStats(uint senderId, int lossPercent, int jitterMs)
    {
        var buffer = new byte[10];
        buffer[0] = (byte)PacketType.Stats;
        buffer[1] = Version;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(2, 4), senderId);
        buffer[6] = (byte)Math.Clamp(lossPercent, 0, 100);
        BinaryPrimitives.WriteInt16LittleEndian(buffer.AsSpan(7, 2), (short)Math.Clamp(jitterMs, 0, 2000));
        return buffer;
    }

    public static byte[] BuildAudio(uint senderId, uint sequence, ReadOnlySpan<byte> payload)
    {
        var buffer = new byte[10 + payload.Length];
        buffer[0] = (byte)PacketType.Audio;
        buffer[1] = Version;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(2, 4), senderId);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(6, 4), sequence);
        payload.CopyTo(buffer.AsSpan(10));
        return buffer;
    }

    public static bool TryParse(ReadOnlySpan<byte> buffer, out PacketType type, out uint senderId, out uint sequence, out ReadOnlySpan<byte> payload)
    {
        type = 0;
        senderId = 0;
        sequence = 0;
        payload = ReadOnlySpan<byte>.Empty;

        if (buffer.Length < 2)
        {
            return false;
        }

        type = (PacketType)buffer[0];
        if (buffer[1] != Version)
        {
            return false;
        }

        switch (type)
        {
            case PacketType.Hello:
            case PacketType.Accept:
            case PacketType.KeepAlive:
                if (buffer.Length < 6)
                {
                    return false;
                }
                senderId = BinaryPrimitives.ReadUInt32LittleEndian(buffer.Slice(2, 4));
                return true;
            case PacketType.Stats:
                if (buffer.Length < 10)
                {
                    return false;
                }
                senderId = BinaryPrimitives.ReadUInt32LittleEndian(buffer.Slice(2, 4));
                return true;
            case PacketType.Audio:
                if (buffer.Length < 10)
                {
                    return false;
                }
                senderId = BinaryPrimitives.ReadUInt32LittleEndian(buffer.Slice(2, 4));
                sequence = BinaryPrimitives.ReadUInt32LittleEndian(buffer.Slice(6, 4));
                payload = buffer.Slice(10);
                return true;
            default:
                return false;
        }
    }
}
