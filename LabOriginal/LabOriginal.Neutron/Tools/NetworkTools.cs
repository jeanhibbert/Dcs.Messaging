using System;
using System.Net.Sockets;

namespace LabOriginal.Neutron.Tools
{
    // NOTE: the original NetworkTools source was not available. This is a minimal stand-in
    // providing the one member the framework calls (CloseConnectionFully).
    public static class NetworkTools
    {
        public static void CloseConnectionFully(TcpClient tcpClient)
        {
            if (tcpClient == null) return;

            try
            {
                if (tcpClient.Client != null && tcpClient.Client.Connected)
                {
                    tcpClient.Client.Shutdown(SocketShutdown.Both);
                }
            }
            catch (Exception)
            {
                // ignore - the socket may already be closed or disposed
            }

            try
            {
                tcpClient.Close();
            }
            catch (Exception)
            {
                // ignore
            }
        }
    }
}
