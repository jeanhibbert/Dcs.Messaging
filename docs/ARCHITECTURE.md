# Architecture

This document describes the internal architecture of Dcs.Messaging, with diagrams illustrating how the components fit together.

## Builder Class Hierarchy

Applications create a session by instantiating a transport-specific builder. All three builders produce an `IMessagingSessionBuilder` with an identical API surface, so all downstream code is transport-agnostic.

```mermaid
classDiagram
    class IMessagingSessionBuilder {
        <<interface>>
        +IEndpointDetailsFactory EndpointDetailsFactory
        +IEndpointDetailsProvider EndpointDetailsProvider
        +IMessageFactory MessageFactory
        +IMessagingService MessagingService
        +IDisposable MessagingTransport
        +IRequestResponder RequestResponder
        +IBinarySerializer Serializer
    }

    class MessagingSessionBuilderBase {
        <<abstract>>
        #MessagingSessionBuilderBase(sessionName, provider, sessionId, transport, endpointProvider)
        #MessagingSessionBuilderBase(sessionName, provider, sessionId, transport, endpointProvider, serializer)
    }

    class TcpMessagingSessionBuilder {
        +TcpMessagingSessionBuilder(sessionName, provider, TcpSessionOptions)
    }

    class GrpcMessagingSessionBuilder {
        +GrpcMessagingSessionBuilder(sessionName, provider, GrpcSessionOptions)
    }

    class SbeMessagingSessionBuilder {
        +SbeMessagingSessionBuilder(sessionName, provider, SbeSessionOptions)
        +SbeMessagingSessionBuilder(sessionName, provider, SbeSessionOptions, SbeBinarySerializer)
    }

    IMessagingSessionBuilder <|.. MessagingSessionBuilderBase
    MessagingSessionBuilderBase <|-- TcpMessagingSessionBuilder
    MessagingSessionBuilderBase <|-- GrpcMessagingSessionBuilder
    MessagingSessionBuilderBase <|-- SbeMessagingSessionBuilder
```

The abstract base class (`MessagingSessionBuilderBase`) wires the common services:

- `MessageEndpointDetailsFactory` -- creates endpoint detail objects
- `MessageFactory` -- creates `IMessage` instances stamped with the session ID
- `IBinarySerializer` -- `ProtobufNetBinarySerializer` by default. `SbeMessagingSessionBuilder` passes an `SbeBinarySerializer` through the second constructor.
- `MessagingService` -- routes send/receive through the endpoint provider
- `RequestResponder` -- request/response correlation layer

Each concrete builder creates its transport session and endpoint provider (and, if it needs one, its serializer), then passes them to the base constructor.

---

## Internal Layering

The framework is structured in layers. Application code interacts with the public API; the transport layer is pluggable underneath.

```mermaid
flowchart TB
    subgraph appLayer [Application Layer]
        APP["Your Code<br/>(services, handlers)"]
    end

    subgraph apiLayer [Public API]
        ISB["IMessagingSessionBuilder"]
        IMS["IMessagingService<br/>.Send() / .GetMessageStream()"]
        IRR["IRequestResponder<br/>.GetRespondableRequestStream()"]
        SER["IBinarySerializer<br/>.Serialize() / .Deserialize()"]
        MF["IMessageFactory<br/>.Create()"]
    end

    subgraph plumbing [Plumbing]
        EP["IEndpointProvider"]
        END["IEndpoint<br/>.Send() / .MessageStream"]
    end

    subgraph transport [Transport Layer - swappable]
        direction LR
        subgraph socketPath [Raw TCP sockets]
            BASE["TcpTransportSessionBase<br/>accept / connect, routing,<br/>per-connection read loop"]
            TCPS["TcpMessagingSession<br/>length-prefixed protobuf"]
            SBES["SbeMessagingSession<br/>SOFH + SBE"]
        end
        subgraph grpcPath [gRPC]
            GRPCS["GrpcMessagingSession<br/>bidirectional streaming<br/>over HTTP/2"]
        end
    end

    subgraph wire [Wire Formats]
        ENV["TransportEnvelope<br/>(protobuf-net)"]
        SENV["SBE TransportEnvelope<br/>(SbeTransportCodec)"]
    end

    APP --> ISB
    ISB --> IMS
    ISB --> IRR
    ISB --> SER
    ISB --> MF
    IMS --> EP
    EP --> END
    END --> TCPS
    END --> SBES
    END --> GRPCS
    TCPS -.inherits.-> BASE
    SBES -.inherits.-> BASE
    TCPS --> ENV
    GRPCS --> ENV
    SBES --> SENV
```

### Key interfaces

| Interface | Responsibility |
|-----------|---------------|
| `IMessagingSessionBuilder` | Entry point. Provides all services needed by application code. |
| `IMessagingService` | Send messages to and receive message streams from logical endpoints. |
| `IRequestResponder` | Higher-level request/response pattern with automatic correlation. |
| `IBinarySerializer` | Serialize/deserialize application payloads (protobuf-net, or SBE for the SBE builder). |
| `IMessageFactory` | Create `IMessage` instances with session identity and properties. |
| `IEndpointProvider` | Map `IEndpointDetails` to `IEndpoint` instances (transport-specific). |
| `IEndpoint` | Low-level send/receive on a single logical endpoint within a session. |

---

## Message Send Flow

When application code calls `MessagingService.Send()`, the message travels through these layers:

```mermaid
sequenceDiagram
    participant App as Application Code
    participant Svc as IMessagingService
    participant EProv as IEndpointProvider
    participant EP as IEndpoint
    participant Sess as Session (TCP, gRPC or SBE)
    participant Factory as TransportEnvelopeFactory
    participant Wire as Network

    App->>Svc: Send(message, endpointDetails)
    Svc->>EProv: GetEndpoint(endpointDetails)
    EProv-->>Svc: IEndpoint
    Svc->>EP: Send(message, targetSessionId)
    EP->>Sess: Send(endpointDetails, message, targetSessionId)
    alt TCP transport
        Sess->>Factory: FromMessage(address, message, targetSessionId)
        Factory-->>Sess: TransportEnvelope
        Sess->>Wire: 4-byte length prefix + protobuf bytes
    else gRPC transport
        Sess->>Factory: FromMessage(address, message, targetSessionId)
        Factory-->>Sess: TransportEnvelope
        Sess->>Wire: IAsyncEnumerable stream item (HTTP/2)
    else SBE transport
        Sess->>Sess: SbeTransportCodec.EncodeFrame (directly from IMessage)
        Sess->>Wire: SOFH + SBE TransportEnvelope
    end
```

### Targeting

- If `targetSessionId` is set, the message is delivered only to the connection whose remote session matches that ID.
- If `targetSessionId` is null/empty and the sender is a **server**, the message is broadcast to all connected clients.
- If the sender is a **client**, messages go to the single server connection.

---

## Message Receive Flow

Incoming messages are deserialized by the session, filtered by endpoint address and target session ID, and pushed into an Rx `ISubject`. Application code subscribes via `GetMessageStream()`.

```mermaid
sequenceDiagram
    participant Wire as Network
    participant Sess as Session (TCP, gRPC or SBE)
    participant Factory as TransportEnvelopeFactory
    participant Subject as ISubject (Rx)
    participant EP as IEndpoint
    participant Svc as IMessagingService
    participant App as Application Code

    Wire->>Sess: incoming bytes / stream item
    alt TCP transport
        Sess->>Sess: ReadEnvelopeAsync (length-prefixed protobuf)
        Sess->>Factory: ToMessage(envelope)
        Factory-->>Sess: IMessage
    else gRPC transport
        Sess->>Sess: await foreach on IAsyncEnumerable
        Sess->>Factory: ToMessage(envelope)
        Factory-->>Sess: IMessage
    else SBE transport
        Sess->>Sess: SbeTransportCodec.ReadFrameAsync (SOFH + SBE, decoded straight to IMessage)
    end
    Sess->>Subject: OnNext(IncomingTransportMessage)
    Note over Subject: filtered by endpoint address<br/>and target session ID
    App->>Svc: GetMessageStream(endpointDetails)
    Svc->>EP: .MessageStream
    EP->>Sess: GetMessageStream(endpointDetails)
    Sess-->>App: IObservable of IMessage (filtered stream)
```

---

## Request/Response Pattern

The `IRequestResponder` provides a higher-level abstraction over send/receive for correlated request/response flows.

```mermaid
sequenceDiagram
    participant Client as Client App
    participant CMF as MessageFactory
    participant CSvc as IMessagingService (client)
    participant Network as Network
    participant SSvc as IMessagingService (server)
    participant SRR as IRequestResponder (server)
    participant Handler as Server Handler

    Client->>CMF: Create(payload, ttl, persistent)
    Client->>Client: Set CorrelationId property
    Client->>CSvc: Send(message, requestEndpoint)
    CSvc->>Network: TransportEnvelope
    Network->>SSvc: incoming envelope
    SSvc->>SRR: GetRespondableRequestStream
    SRR->>Handler: IRespondableRequest
    Handler->>Handler: process request
    Handler->>SRR: request.Respond(response)
    SRR->>SSvc: Send(responseMessage, responseEndpoint, senderSessionId)
    SSvc->>Network: TransportEnvelope (targeted to client)
    Network->>CSvc: incoming envelope
    CSvc->>Client: filtered by CorrelationId
```

### How it works

1. The **client** creates a message, stamps it with a `CorrelationId` property, and sends it to the request endpoint.
2. The **server** subscribes to `IRequestResponder.GetRespondableRequestStream<TReq, TRes>(requestEndpoint, responseEndpoint)`. Each incoming request is exposed as an `IRespondableRequest<TReq, TRes>`.
3. The server handler calls `request.Respond(response)`, which serializes the response, stamps it with the same `CorrelationId`, and sends it back via the response endpoint targeted to the original sender's session ID.
4. The **client** filters incoming messages on the response endpoint by `CorrelationId` to match the reply.

---

## Wire Formats

### Protobuf envelope (TCP and gRPC)

TCP and gRPC use the same protobuf-serialized envelope (`TransportEnvelope`):

| ProtoMember | Field | Type | Description |
|-------------|-------|------|-------------|
| 1 | `EndpointAddress` | `string` | Logical endpoint the message belongs to |
| 2 | `SenderSessionId` | `string` | Session ID of the sender |
| 3 | `TargetSessionId` | `string` | If set, message is delivered only to this session |
| 4 | `Tag` | `string` | Optional message tag |
| 5 | `Properties` | `List<TransportPropertyValue>` | Key-value metadata (correlation ID, TTL, etc.) |
| 6 | `Payload` | `byte[]` | Serialized application payload |

Each `TransportPropertyValue` carries a `Key`, `Kind` (bool/double/int/string), and the corresponding typed value field. This allows message properties to round-trip across the wire without losing type information.

#### TCP framing

```
[4 bytes: big-endian int32 payload length][N bytes: protobuf-serialized TransportEnvelope]
```

#### gRPC framing

The `TransportEnvelope` is sent as individual items in a bidirectional `IAsyncEnumerable<TransportEnvelope>` stream. HTTP/2 handles framing.

### SBE envelope

The SBE transport does not use the protobuf `TransportEnvelope`. `SbeTransportCodec` writes an SBE message straight from the `IMessage` into one exactly-sized buffer, and decodes incoming frames straight back into an `IMessage`. The schema is in `Dcs.Messaging/Sbe/Schemas/dcs-messaging-transport.xml` (schema id 1, template id 1, version 0).

```
SOFH     [uint32 BE frame length incl. SOFH][uint16 BE encoding type 0x5BE0]
header   [uint16 blockLength][uint16 templateId][uint16 schemaId][uint16 version]     (little-endian from here on)
block    [int32 timeToLiveMs][uint8 isPersistent]
group    [uint16 blockLength][uint16 numInGroup]
           numInGroup x { [double numericValue][uint8 kind] [key] [stringValue] }
data     [endpointAddress][senderSessionId][targetSessionId][tag][payload]
```

| Part | Field | Encoding | Notes |
|------|-------|----------|-------|
| Root block | `timeToLiveMs` | `int32`, null = `int.MinValue` | `TransportPropertyKeys.TimeToLiveMs`. Fixed field because `MessageFactory` sets it on every message. |
| Root block | `isPersistent` | `uint8`, 0/1, null = 255 | `TransportPropertyKeys.IsPersistent`, also set on every message. |
| `properties` group | `numericValue` | `double` | Value for `Bool` (0/1), `Int` and `Double` kinds. Every int32 fits exactly in a double. |
| `properties` group | `kind` | `uint8` enum | `Bool`, `Int`, `Double` or `String` |
| `properties` group | `key`, `stringValue` | `varStringEncoding` | Other value types are sent as invariant-culture strings, as in the protobuf envelope. |
| Data | `endpointAddress`, `senderSessionId`, `targetSessionId`, `tag` | `varStringEncoding` (uint16 length + UTF-8) | An empty `targetSessionId` means broadcast. |
| Data | `payload` | `varDataEncoding` (uint32 length + bytes) | Application payload from the session's `IBinarySerializer` |

Decoders honour the `blockLength` values on the wire, so a newer schema version can append fields to the root block or the group entries and older readers will skip them.

With the SBE builder the application payload is SBE too. The schema for the sample DTOs is `Dcs.Messaging.Common/Sbe/dcs-messaging-common.xml` (schema id 2):

| Template | DTO | Root block | Variable data |
|----------|-----|------------|---------------|
| 1 | `TestAlertDto` | `int32 messageId` | `serverName`, `message` |
| 2 | `PricingRequestDto` | `double spotRate`, `CurrencyPair` | `requestedBy`, `tenor` |
| 3 | `ForwardPricingDto` | `double bidRate`, `double askRate`, `CurrencyPair`, `uint8 isContra` | none (fixed 35 bytes including header) |

`CurrencyPair` is a composite: `char[3] baseCcy`, `char[3] quoteCcy`, and optional `int32 decimalPlaces`.

---

## Endpoint Configuration

Endpoints are logical channels identified by string keys. An `IEndpointDetailsProvider` maps keys to `IEndpointDetails` objects (address + type).

The sample apps use `TestEndpointDetailsProvider` with three pre-configured endpoints:

| Key | Address | Used for |
|-----|---------|----------|
| `TEST_ALERTS` | `Test/Alerts` | Server broadcasts alert messages (pub/sub) |
| `TEST_PRICING_REQUESTS` | `Test/Pricing/Requests` | Client sends pricing requests |
| `TEST_PRICING_RESPONSES` | `Test/Pricing/Responses` | Server sends pricing responses back |

To add your own endpoints, implement `IEndpointDetailsProvider` or extend `TestEndpointDetailsProvider`.

---

## Adding a New Transport

The framework is designed to be extensible. To add a new transport (e.g. WebSocket, named pipes):

1. Create a new session class (like `TcpMessagingSession` or `GrpcMessagingSession`) that implements `IDisposable` and exposes `GetMessageStream()` and `Send()` methods. If the new transport only changes the encoding of frames over raw TCP sockets, derive from `TcpTransportSessionBase` and implement `EncodeFrame` and `ReadFrameAsync`, as `SbeMessagingSession` does.
2. Create a matching `IEndpointProvider` and `IEndpoint` implementation.
3. Create a new builder class extending `MessagingSessionBuilderBase`, passing your session and endpoint provider (and optionally an `IBinarySerializer`) to the base constructor.

No existing code needs to change -- the Open/Closed principle is preserved by the builder hierarchy.
