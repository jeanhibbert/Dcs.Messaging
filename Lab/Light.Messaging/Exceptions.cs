using System;

namespace Light.Messaging;

/// <summary>
/// The connection closed, or a request could not be delivered.
/// </summary>
public class MessagingException : Exception
{
    public MessagingException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// The responder threw while handling the request; <see cref="Exception.Message"/> is the remote exception's message.
/// </summary>
public sealed class RemoteException : MessagingException
{
    public RemoteException(string message)
        : base(message)
    {
    }
}
