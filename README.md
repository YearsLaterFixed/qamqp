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

Usage notes
------------

#### credentials
There is no default `guest`/`guest` credential. Call `setUsername()` and
`setPassword()` (or supply a `QAmqpAuthenticator` via `setAuth()`) before
connecting, or pass them in the connection URI.

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
`AcceptedForWrite`, `Queued`, `RejectedLimit`, or `NotConnected`. `Queued` and
`AcceptedForWrite` do not mean the broker received or accepted the message; use
publisher confirms when broker acknowledgement is required. A deferred publish
queue is limited to 1,000 messages or 16 MiB of fully serialized data per
exchange. The client also bounds its asynchronous write backlog; its default
budget is 16 MiB and can be changed with `QAmqpClient::setMaxWriteQueueBytes()`.
Setting the client budget to zero disables writes. Exceeding either limit is
reported to the caller instead of silently dropping a publish. Writes are
drained asynchronously, so a slow peer does not block the Qt event loop.

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
