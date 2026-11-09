using System;
using System.Collections.Concurrent;
using System.Text;

namespace Light.Messaging;

/// <summary>
/// A named destination, such as <c>"Prices/EURUSD"</c>. Only the 32-bit <see cref="Id"/> travels on the wire. It is
/// derived from the address with FNV-1a, so every process computes the same id without any registration handshake.
/// Declare endpoints once, as <c>static readonly</c> fields in a contracts assembly shared by clients and servers.
/// </summary>
public readonly struct Endpoint : IEquatable<Endpoint>
{
    private static readonly ConcurrentDictionary<int, string> AddressesById = new();

    /// <exception cref="ArgumentException">The address hashes to the same id as another address in this process.</exception>
    public Endpoint(string address)
    {
        ArgumentException.ThrowIfNullOrEmpty(address);
        Address = address;
        Id = ComputeId(address);

        var registered = AddressesById.GetOrAdd(Id, address);
        if (!string.Equals(registered, address, StringComparison.Ordinal))
        {
            throw new ArgumentException($"Endpoint '{address}' has the same id as '{registered}'. Rename one of them.", nameof(address));
        }
    }

    public string Address { get; }

    public int Id { get; }

    public bool Equals(Endpoint other) => Id == other.Id;

    public override bool Equals(object obj) => obj is Endpoint other && Equals(other);

    public override int GetHashCode() => Id;

    public override string ToString() => Address;

    public static bool operator ==(Endpoint left, Endpoint right) => left.Equals(right);

    public static bool operator !=(Endpoint left, Endpoint right) => !left.Equals(right);

    /// <summary>
    /// The address for an id, if this process has created that endpoint; otherwise the id itself.
    /// </summary>
    internal static string Describe(int id)
    {
        return AddressesById.TryGetValue(id, out var address) ? address : $"#{id}";
    }

    /// <summary>
    /// FNV-1a over the UTF-8 bytes: stable across processes, platforms and .NET versions, unlike
    /// <see cref="string.GetHashCode()"/>.
    /// </summary>
    internal static int ComputeId(string address)
    {
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;

        var hash = offsetBasis;
        foreach (var b in Encoding.UTF8.GetBytes(address))
        {
            hash = (hash ^ b) * prime;
        }

        return (int)hash;
    }
}
