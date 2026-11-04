[![QAMQP CI](https://github.com/YearsLaterFixed/qamqp/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/YearsLaterFixed/qamqp/actions/workflows/ci.yml)

# QAMQP 1.0.0

An asynchronous Qt AMQP 0.9.1 client, maintained by
[YearsLaterFixed](https://github.com/YearsLaterFixed/qamqp), derived from
[mbroadst/qamqp](https://github.com/mbroadst/qamqp). RabbitMQ is the tested broker.

| Tested baseline | Coverage |
| --- | --- |
| Debian 11, Qt 5.15.2, GCC 10.2.1, C++11 | Full local broker/TLS/reconnect suite and source-package consumers |
| Linux CI, Qt 5.15.2 and Qt 6.5.3 | Parser/flow, shared/static CMake packages and Git-free qmake builds |
| RabbitMQ 3.13, Qt 5.15.2 | Delivery, confirms, overflow, reconnect, verified TLS and multi-channel soak |
| Qt 5.15.2 with AddressSanitizer/UndefinedBehaviorSanitizer | Parser and controlled-socket lifecycle tests |

Qt 6 uses its own required language level. Historical upstream platform claims
are not a fresh Windows/macOS certification. See [CHANGELOG.md](CHANGELOG.md)
before migrating from ABI 0, and [LICENSING.md](LICENSING.md) before distribution.
Examples are in [tutorials](tutorials).

## Testing

GitHub Actions runs the parser and channel-flow suites without a broker on Qt
5.15.2 and Qt 6.5.3. A separate Qt 5.15.2 job starts RabbitMQ with the test TLS
certificates and runs the client, exchange, queue, and channel integration
suites, including reconnect tests.

Build the library and all test targets out of tree with qmake:

```bash
mkdir -p build-qmake
cd build-qmake
qmake ../src/src.pro
make -j2
cd ..
for test in qamqpparser qamqpflow qamqpclient qamqpexchange qamqpqueue qamqpchannel; do
	mkdir -p "build-tests/$test"
	(cd "build-tests/$test" && qmake "../../tests/auto/$test/$test.pro" \
		"LIBS+=-L$PWD/../../build-qmake" && make -j2)
	done
	export LD_LIBRARY_PATH="$PWD/build-qmake${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

Run `tst_qamqpparser` and `tst_qamqpflow` without RabbitMQ. The other test
executables require RabbitMQ on `localhost:5672`; `tst_qamqpclient` TLS tests
also require a TLS listener on port 5671. The CI workflow shows the service
configuration and the RabbitMQ control wrapper needed by reconnect tests.
The reconnect cases are registered QtTest tests: one restarts the dedicated test
broker and verifies retained named channels, rebind/reconsume, and delivery;
another checks fixed-delay recovery from connection refusal. Run them only
against a disposable broker controlled by the test `rabbitmqctl` wrapper.
TLS tests require fresh fixtures instead of the legacy checked-in certificates:

```bash
bash tests/files/ci/generate-certificates.sh /tmp/qamqp-certificates
export QAMQP_TEST_CA_CERT=/tmp/qamqp-certificates/testca/cacert.pem
```

Mount that directory at `/certs` in the test broker. The generated server
certificate identifies `localhost` and `127.0.0.1`; tests explicitly verify the
peer and reject untrusted certificates and hostname mismatches without ignoring
SSL errors. Keys are ephemeral test fixtures, not production credentials. CI
generates them for every run and checks that all restored test names are
discoverable before executing the integration suites.
Receive-overflow integration tests exercise acknowledged and `coNoAck` consumers
through repeated count, payload-byte and body-size saturation/recovery cycles,
including empty and multi-frame deliveries, explicit reject/requeue, and
disconnect/reconsume. No-ack discard tests verify that no broker redelivery is
available. Separate broker tests distinguish local publish admission from
publisher confirmation and delivery. Controlled socket tests force stopped and
partial writes through 32 recovery cycles per deferred state, checking
metadata-inclusive accounting, per-exchange FIFO, frame integrity, control
traffic and event-loop progress. These deterministic regressions supplement,
but do not replace, longer-running release soak tests.

Build and install with CMake (QAMQP 1.0.0 and later):

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build
cmake --install build
```

Consumers can use `find_package(QAMQP CONFIG REQUIRED)` and link the exported
`QAMQP::qamqp` target. Qt Core and Network are propagated as target dependencies.
The legacy qmake project remains supported.

For a vendored source checkout, use `add_subdirectory(third_party/qamqp)` and
link `QAMQP::qamqp` instead of maintaining another source list or library version.
Set `BUILD_SHARED_LIBS=ON` for the shared ABI-1 library or `OFF` for static linking.
Run the Git-free install/consumer gate with
`bash tests/files/ci/check-release.sh /tmp/qamqp-package-check`.

Incoming method validation covers connection start/secure/tune/open-ok/close,
channel open-ok/flow/close, exchange and queue replies, basic delivery/get/return,
consumer replies, publisher acknowledgements, confirm-select-ok, and transaction
replies. Methods are validated for complete arguments, bounded strings/tables,
defined packed bits, applicable value constraints, and no trailing arguments
before handlers can mutate state or emit application signals. Unsupported method
identifiers, wrong-direction methods, and malformed arguments are rejected with
`FrameError`; unimplemented protocol extensions are not implicitly accepted.

## Essential Contracts

- Configure credentials explicitly; keep each client and its channels on one event loop.
- Choose receive limits before consuming. No-ack discard cannot be requeued.
- Handle every publish admission result. Queued/written is not broker-confirmed.
- Rebuild ABI-0 consumers and update deployment for SONAME 1.
- Preserve the license/notice package and corresponding library source/build scripts.

<details>
<summary>Detailed buffering, threading and recovery contract</summary>

#### credentials
There is no default `guest`/`guest` credential. Call `setUsername()` and
`setPassword()` (or supply a `QAmqpAuthenticator` via `setAuth()`) before
connecting, or pass them in the connection URI.

#### protocol limits
`QAmqpClient::channelMax()` and `heartbeatDelay()` expose signed 32-bit values
so setters can reject negative or above-range input before narrowing to the
unsigned 16-bit AMQP wire fields. Valid values are 0-65,535. Zero means
unlimited channels or disabled heartbeats, as defined by the protocol. Callers
should handle the setter's range contract and update code that stored these
properties as `qint16` to use at least `qint32`.

#### heartbeat liveness
After heartbeat negotiation, the client sends heartbeats every half timeout
and monitors inbound activity with a monotonic clock. Ordinary inbound traffic
counts as activity; peers do not need to send heartbeat frames while other
traffic is arriving. A negotiated zero disables both heartbeat timers.
If no inbound activity arrives within the negotiated timeout, the client aborts
the socket and emits `error(ConnectionForcedError)` with a receive-heartbeat
timeout description. Automatic reconnect schedules one pending attempt when
enabled, using the configured reconnect delay (or a one-second fallback).
Expiry checks run at most one second apart on the owning event loop. Closing,
disconnecting, and starting a new connection stop the old timers and invalidate
the old activity timestamp; a new negotiation starts a fresh deadline.

#### thread affinity
A `QAmqpClient`, and every channel, exchange and queue it creates, must be used
from the thread that owns the client's event loop. Most of the API is invoked
directly rather than through queued signal/slot connections, so calling it from
another thread is not safe. Use one `QAmqpClient` per thread rather than sharing
a single client.

#### message buffering
`QAmqpQueue` buffers delivered messages in memory. Configure limits before
consuming when the application has a memory budget:

```cpp
queue->setMaxBufferedMessages(100);
queue->setMaxBufferedBytes(16 * 1024 * 1024);
queue->setMaxMessageSize(8 * 1024 * 1024);
```

A zero message-count or byte limit means unlimited. The default count and byte
limits are unlimited for compatibility; the default individual message limit is
`AMQP_MESSAGE_MAX` (128 MiB). Configure `maxMessageSize` lower for constrained
devices. The queue accounts queued and reassembling payload bytes when deciding
whether to accept another message.

When a limit is exceeded, the incoming message is discarded and
`messageDiscarded(deliveryTag, reason)` is emitted; the library does not silently
drop it. With acknowledged consumption, the application can use the delivery tag
to choose whether to reject or requeue it. With `coNoAck`, the broker has already
auto-acknowledged the delivery, so it cannot be redelivered. QoS/prefetch does not
bound memory for `coNoAck` consumers. Drain the queue promptly with `dequeue()`;
`QAmqpQueue` no longer publicly inherits `QQueue` so callers cannot bypass byte
accounting through inherited mutators.

#### publish buffering and delivery
`QAmqpExchange::publish()` returns a `PublishResult` describing local admission:
`AcceptedForWrite`, `Queued`, `RejectedLimit`, `NotConnected`, or `RejectedInvalid`. `Queued` and
`AcceptedForWrite` do not mean the broker received or accepted the message; use
publisher confirms when broker acknowledgement is required. A deferred publish
queue is limited to 1,000 messages or 16 MiB of conservative reserved data per
exchange. The client also bounds its asynchronous write backlog; its default
budget is 16 MiB and can be changed with `QAmqpClient::setMaxWriteQueueBytes()`.
Setting the client budget to zero disables application publishes, not control traffic. Exceeding either limit is
reported to the caller instead of silently dropping a publish. Writes are
drained asynchronously, so a slow peer does not block the Qt event loop.
Protocol traffic has a separate, fixed 64 KiB reserve; it is not included in
`bufferedWriteBytes()` or the application write budget. Both budgets include
their corresponding bytes already handed to Qt but not yet reported written.
One socket writer finishes partial frames before switching traffic. Heartbeats
and eligible traffic on other channels may pass queued publishes at complete
frame boundaries; same-channel methods retain their ordering behind an admitted
publish's method/header/body sequence. FIFO is preserved for queued publishes.
An exhausted control reserve, failed socket write, or pending control traffic
without control-byte progress for 30 seconds (or the negotiated heartbeat
timeout, if shorter) emits `error(ResourceError)` and aborts the connection.
Automatic reconnect is scheduled when enabled. Control priority cannot make a
stalled peer drain, and never introduces blocking writes or unbounded buffering.
Short strings exceeding 255 UTF-8 bytes are rejected before admission, without
truncation or a write reservation. Invalid publishes return `RejectedInvalid`
and emit `publishRejected(InvalidShortString)` once.
Deferred publishes retain their payload and serialized method/header frames,
not body frames built for an earlier connection. Admission reserves twice the
worst-case wire size at the AMQP minimum frame limit (4096 bytes, including
eight-byte frame overhead): one copy for retained data and one for bounded
serialization staging. This conservative charge counts against both the client
budget and the per-exchange deferred-byte limit. It can reduce the maximum
admissible payload compared with a wire-only budget.
Body frames are rebuilt for the current negotiated limit before transfer to
the writer; unused reservation is released during that transfer. Nonfragmentable
method/header frames must fit the conservative limit before a publish is queued,
or the current limit for an immediately written publish. Otherwise admission
returns `RejectedLimit` and emits `publishRejected(FrameSizeLimitExceeded)`.
An unexpected permanent framing/encoding failure of an already queued entry
releases its reservation and emits `publishFailed(error)` once. Transient
disconnection keeps a deferred entry reserved for replay. Once ownership has
transferred to the writer, transport failure is reported through client errors
and `writeQueueDiscarded`; those bytes are not retained for replay. Neither
replay nor local admission implies exactly-once delivery or broker confirmation.
`cancelPendingPublishes()` explicitly cancels only this exchange's deferred
publishes, returns their count, and emits
`pendingPublishesCancelled(count, reservedBytes)` once when that count is nonzero.
Calling it again on an empty queue returns zero without emitting a notification.
Cancellation releases the corresponding client budget immediately, but cannot
retract publishes already admitted to the socket writer or delivered to a broker.
Destroying an exchange also releases its remaining deferred reservations without
changing another exchange's reservations or the writer backlog. Deferred entries
and their reservations remain intact across disconnect/reset until replay,
explicit cancellation, or exchange destruction.

</details>

## Release Verification

The release gates build Git-free source archives with CMake and qmake, install
shared/static packages into clean prefixes, run external consumers, inspect
version/SONAME and notice contents, and collect sanitizer and reconnect-soak
evidence. See [docs/RELEASE.md](docs/RELEASE.md) for the exact commands and gates.
Release assets carry checksums; use the tagged source and record its commit.

<details>
<summary>Inherited protocol-method reference</summary>

This reference describes the inherited API surface, not certification of every
authentication mechanism or broker extension. The validation and migration
contracts above take precedence over historical platform/support claims.

### AMQP Support

#### connection
| method | supported |
| ---    | ---       |
| connection.start      | ✓ |
| connection.start-ok   | ✓ |
| connection.secure     | ✓ |
| connection.secure-ok  | ✓ |
| connection.tune       | ✓ |
| connection.tune-ok    | ✓ |
| connection.open       | ✓ |
| connection.open-ok    | ✓ |
| connection.close      | ✓ |
| connection.close-ok   | ✓ |

#### channel
| method | supported |
| ------ | --------- |
| channel.open          | ✓ |
| channel.open-ok       | ✓ |
| channel.flow          | ✓ |
| channel.flow-ok       | ✓ |
| channel.close         | ✓ |
| channel.close-ok      | ✓ |

#### exchange
| method | supported |
| ------ | --------- |
| exchange.declare      | ✓ |
| exchange.declare-ok   | ✓ |
| exchange.delete       | ✓ |
| exchange.delete-ok    | ✓ |

#### queue
| method | supported |
| ------ | --------- |
| queue.declare         | ✓ |
| queue.declare-ok      | ✓ |
| queue.bind            | ✓ |
| queue.bind-ok         | ✓ |
| queue.unbind          | ✓ |
| queue.unbind-ok       | ✓ |
| queue.purge           | ✓ |
| queue.purge-ok        | ✓ |
| queue.delete          | ✓ |
| queue.delete-ok       | ✓ |

#### basic
| method | supported |
| ------ | --------- |
| basic.qos             | ✓ |
| basic.consume         | ✓ |
| basic.consume-ok      | ✓ |
| basic.cancel          | ✓ |
| basic.cancel-ok       | ✓ |
| basic.publish         | ✓ |
| basic.return          | ✓ |
| basic.deliver         | ✓ |
| basic.get             | ✓ |
| basic.get-ok          | ✓ |
| basic.get-empty       | ✓ |
| basic.ack             | ✓ |
| basic.reject          | ✓ |
| basic.recover         | ✓ |

#### tx
| method | supported |
| ------ | --------- |
| tx.select             | X |
| tx.select-ok          | X |
| tx.commit             | X |
| tx.commit-ok          | X |
| tx.rollback           | X |
| tx.rollback-ok        | X |

#### confirm
| method | supported |
| ------ | --------- |
| confirm.select        | ✓ |
| confirm.select-ok     | ✓ |

</details>

## Credits
* Thank you to [@fuCtor](https://github.com/fuCtor) for the original implementation work.
