namespace Light.Messaging.Internal;

internal enum FrameKind : byte
{
    Hello = 1,
    Publish = 2,
    Request = 3,
    Response = 4,
    ErrorResponse = 5,
    Subscribe = 6,
    Unsubscribe = 7,

    /// <summary>
    /// The sender's last frame before it ends its stream; the payload is the UTF-8 reason.
    /// </summary>
    Goodbye = 8
}

/// <summary>
/// Frame layout, little-endian, identical on TCP and gRPC:
/// <code>
/// uint32 bodyLength      bytes after this field
/// uint8  kind            FrameKind
/// int32  endpointId      Endpoint.Id (0 for Hello)
/// int64  correlationId   Request, Response and ErrorResponse only
/// ...    payload         serialized message | Hello: uint16 version + UTF-8 name | ErrorResponse, Goodbye: UTF-8 text
/// </code>
/// A publish therefore costs 9 bytes of header, and a request or response 17.
/// <para>
/// Graceful close: the closing side sends Goodbye after its last frame and ends its stream. The other side finishes
/// sending what it has queued, replies with Goodbye and ends its stream too. Each side has then received everything
/// before either tears the transport down, so no in-flight data is lost to a reset.
/// </para>
/// </summary>
internal static class Wire
{
    public const ushort ProtocolVersion = 1;
    public const int LengthSize = sizeof(uint);

    /// <summary>
    /// Kind plus endpoint id: the smallest valid body.
    /// </summary>
    public const int MinBodySize = 1 + sizeof(int);

    public static bool HasCorrelation(FrameKind kind)
    {
        return kind is FrameKind.Request or FrameKind.Response or FrameKind.ErrorResponse;
    }

    /// <summary>
    /// Body bytes before the payload.
    /// </summary>
    public static int BodyHeaderSize(FrameKind kind)
    {
        return HasCorrelation(kind) ? MinBodySize + sizeof(long) : MinBodySize;
    }
}

internal sealed record ConnectionSettings(
    string LocalName,
    int QueueCapacity,
    SlowConsumerPolicy SlowConsumerPolicy,
    int MaxFrameBytes,
    int SendBatchBytes);
