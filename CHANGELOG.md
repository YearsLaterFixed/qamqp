# QAMQP 1.0.0

## Compatibility and Migration

- The shared-library ABI is version 1 (SONAME `libqamqp.so.1`). Rebuild consumers;
  do not replace an ABI-0 library in place while retaining old application binaries.
- `QAmqpQueue` no longer derives from `QQueue<QAmqpMessage>`. Use `size()`,
  `isEmpty()`, `head()`, `dequeue()` and `clear()`; inherited mutators are removed
  so callers cannot bypass byte accounting.
- `channelMax` and `heartbeatDelay` public properties/setters use signed 32-bit
  values and accept 0..65535. Invalid input is rejected before wire narrowing.
- Every `publish()` overload returns a local admission result:
  `AcceptedForWrite`, `Queued`, `RejectedLimit`, `NotConnected`, or
  `RejectedInvalid`. Handle rejection explicitly. Admission is not broker
  confirmation; publisher confirms remain a separate protocol mechanism.
- `publishRejected(error)`, `publishFailed(error)`, client write overflow/discard
  signals and `pendingPublishesCancelled(count, reservedBytes)` make rejection,
  permanent deferred failure, transport loss and explicit cancellation observable.
- `cancelPendingPublishes()` cancels only this exchange's deferred entries;
  destruction releases their reservations too. Neither retracts writer-owned bytes.
- AMQP short strings longer than 255 UTF-8 bytes are rejected, not truncated.
- Incoming supported method arguments and complete frames are validated before
  dispatch; unsupported methods/extensions are not implicitly accepted.

## Resource and Recovery Contract

Receive count/byte limits remain unlimited for compatibility. The individual
body cap defaults to 128 MiB. Configure `setMaxBufferedMessages()`,
`setMaxBufferedBytes()` and `setMaxMessageSize()` before consuming. Receive byte
accounting covers payloads, not an absolute allocator/RSS bound. Overflow drops
the incoming delivery and emits `messageDiscarded(deliveryTag, reason)` once.
Acknowledged consumers may explicitly reject/requeue it. `coNoAck` deliveries
cannot be broker-requeued; QoS alone is not a no-ack memory bound.

The application write budget defaults to 16 MiB. Deferred publishes are also
limited to 1000 entries / 16 MiB per exchange. Before admission, deferred entries
reserve twice their worst-case wire size at the 4096-byte AMQP minimum: retained
data and bounded encoding staging, including metadata and frame overhead.
This can reduce the largest admissible payload relative to wire-only accounting.
Nonfragmentable method/header frames must fit the applicable frame limit.
Queued bodies are reframed for the negotiated connection and FIFO is preserved
within an exchange/channel; no cross-thread or global cross-channel order is promised.

A separate fixed 64 KiB control reserve cannot be consumed by application data.
One asynchronous writer preserves partial frames and same-channel publish
sequences. Control-reserve exhaustion, socket-write failure and control-progress
expiry report `ResourceError` and abort; reconnect is scheduled when enabled.

Heartbeats are sent every half negotiated timeout. Inbound activity is tracked
with a monotonic clock; silence for the negotiated timeout reports
`ConnectionForcedError` and aborts. A negotiated zero disables heartbeat timers.
Closing/disconnect/new connection cleans old state. Partial inbound bodies are
released on reset. Retained named objects can be redeclared/rebound/reconsumed
after reconnect. Deferred publishes retain reservations for replay; writer-owned
bytes lost on disconnect are reported and not silently replayed. Exactly-once
delivery is not promised.

## Build and Distribution

Use the maintained CMake target `QAMQP::qamqp` through `add_subdirectory()` or
`find_package(QAMQP 1.0.0 CONFIG REQUIRED)`. Qt Core/Network dependencies and
shared-library definitions propagate to consumers. qmake remains supported.
Library version metadata is 1.0.0 even without Git or with unrelated Git tags.

Validated Linux baselines are Qt 5.15.2 / GCC 10.2.1 / C++11 and Qt 6.5.3
with its required compiler language level. RabbitMQ 3.13 is the integration
baseline. Historical platform claims are not fresh certification for this fork.

See [LICENSING.md](LICENSING.md) for inherited licensing/version discrepancies,
notices and distribution requirements. No wholesale MIT relicensing is claimed.