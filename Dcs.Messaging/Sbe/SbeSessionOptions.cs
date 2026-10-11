namespace Dcs.Messaging.Sbe
{
    /// <summary>
    /// Options for an SBE messaging session. SBE frames travel over raw TCP sockets, so the shape mirrors
    /// <see cref="Tcp.TcpSessionOptions"/>.
    /// </summary>
    public sealed class SbeSessionOptions
    {
        private SbeSessionOptions()
        {
        }

        public string Host { get; set; }

        public SbeSessionMode Mode { get; set; }

        public int Port { get; set; }

        public string SessionId { get; set; }

        public static SbeSessionOptions CreateClient(string host, int port, string sessionId)
        {
            return new SbeSessionOptions
            {
                Host = host,
                Mode = SbeSessionMode.Client,
                Port = port,
                SessionId = sessionId
            };
        }

        public static SbeSessionOptions CreateServer(string host, int port, string sessionId)
        {
            return new SbeSessionOptions
            {
                Host = host,
                Mode = SbeSessionMode.Server,
                Port = port,
                SessionId = sessionId
            };
        }
    }
}
