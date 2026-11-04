[![Build Status](https://travis-ci.org/mbroadst/qamqp.svg?branch=master)](https://travis-ci.org/mbroadst/qamqp)
[![Coverage Status](https://img.shields.io/coveralls/mbroadst/qamqp.svg)](https://coveralls.io/r/mbroadst/qamqp?branch=master)

QAMQP
=============
A Qt5/Qt6 implementation of AMQP 0.9.1, focusing primarily on RabbitMQ support.

Usage
------------
* [hello world](https://github.com/mbroadst/qamqp/tree/master/tutorials/helloworld)
* [pubsub](https://github.com/mbroadst/qamqp/tree/master/tutorials/pubsub)
* [routing](https://github.com/mbroadst/qamqp/tree/master/tutorials/routing)
* [rpc](https://github.com/mbroadst/qamqp/tree/master/tutorials/rpc)
* [topics](https://github.com/mbroadst/qamqp/tree/master/tutorials/topics)
* [work queues](https://github.com/mbroadst/qamqp/tree/master/tutorials/workqueues)

Documentation
------------
Tests checked and integrated against rabbitmq 3.11 (August 1, 2022)
Qt5.6.3 (MSVC2017) 32Bit
Qt5.15.2 (MSVC2019, MSVC2022, MinGW, Clang) 32 and 64Bit
Qt6.5 (MSVC2019, MSVC2022) 64Bit

A good starting point is:
* running a local RabbitMQ,
* browse to http://localhost:15672/#/queues  (guest/guest)
* Start the "receive" sample and see in your browser the "hello" queue appear
* publish a message there

## Testing

GitHub Actions runs the parser and channel-flow suites without a broker on Qt
5.15.2 and Qt 6.5.3. A separate Qt 5.15.2 job starts RabbitMQ with the test TLS
certificates and runs the client, exchange, queue, and channel integration
suites, including reconnect tests.

To build the library and all test targets locally with qmake:

```bash
cd src
qmake src.pro
make -j2
cd ../tests/auto
for test in qamqpparser qamqpflow qamqpclient qamqpexchange qamqpqueue qamqpchannel; do
	(cd "$test" && qmake "$test.pro" && make -j2)
done
```

Run `tst_qamqpparser` and `tst_qamqpflow` without RabbitMQ. The other test
executables require RabbitMQ on `localhost:5672`; `tst_qamqpclient` TLS tests
also require a TLS listener on port 5671. The CI workflow shows the service
configuration and the RabbitMQ control wrapper needed by reconnect tests.

Build and install with CMake (QAMQP 1.0.0 and later):

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build
cmake --install build
```

Consumers can use `find_package(QAMQP CONFIG REQUIRED)` and link the exported
`QAMQP::qamqp` target. Qt Core and Network are propagated as target dependencies.
The legacy qmake project remains supported.

Usage notes
------------

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
queue is limited to 1,000 messages or 16 MiB of fully serialized data per
exchange. The client also bounds its asynchronous write backlog; its default
budget is 16 MiB and can be changed with `QAmqpClient::setMaxWriteQueueBytes()`.
Setting the client budget to zero disables writes. Exceeding either limit is
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

AMQP Support
------------

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

Credits
------------
* Thank you to [@fuCtor](https://github.com/fuCtor) for the original implementation work.
