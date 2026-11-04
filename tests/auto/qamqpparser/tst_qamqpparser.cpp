#include <QtTest/QtTest>
#include <QRandomGenerator>

#include "qamqpframe_p.h"
#include "qamqpclient.h"
#include "qamqpqueue.h"
#include "qamqpqueue_p.h"
#include "qamqptable.h"
#include "qamqpglobal.h"

// Fuzz/boundary tests for the untrusted-input parsing code in qamqpframe.cpp and
// qamqptable.cpp. These do not require a running broker: they exercise the parsers
// directly with crafted and randomized byte buffers, asserting only that the code
// never crashes/hangs/over-allocates on malformed input.
class tst_QAMQPParser : public QObject
{
    Q_OBJECT
private Q_SLOTS:
    void shortStringBoundaries_data();
    void shortStringBoundaries();

    void fieldSizeBoundaries_data();
    void fieldSizeBoundaries();

    void utf8StringRoundTrip_data();
    void utf8StringRoundTrip();

    void truncatedMethodFrame();
    void deeplyNestedArray();

    void messageBodySize_data();
    void messageBodySize();

    void messageBodyChunk_data();
    void messageBodyChunk();
    void queueBufferLimits_data();
    void queueBufferLimits();
    void queueBufferLimitConfiguration();
    void queueBufferAccountingAndOverflow();
    void queueProtocolBodyCapEmitsDiscard();

    void fuzzReadAmqpField();
    void fuzzTableFieldValues();
    void fuzzTableDecode();
    void fuzzMethodFrame();

private:
    static QByteArray randomBytes(QRandomGenerator &rng, int len);
};

QByteArray tst_QAMQPParser::randomBytes(QRandomGenerator &rng, int len)
{
    QByteArray buffer;
    buffer.resize(len);
    for (int i = 0; i < len; ++i)
        buffer[i] = char(rng.bounded(256));
    return buffer;
}

void tst_QAMQPParser::shortStringBoundaries_data()
{
    QTest::addColumn<int>("declaredSize");

    QTest::addColumn<bool>("truncate");

    QTest::newRow("empty") << 0 << false;
    QTest::newRow("ascii-127") << 127 << false;
    QTest::newRow("truncated-payload") << 200 << true;
}

void tst_QAMQPParser::shortStringBoundaries()
{
    QFETCH(int, declaredSize);
    QFETCH(bool, truncate);

    const int actualBytes = truncate ? declaredSize / 4 : declaredSize;

    QByteArray buffer;
    QDataStream out(&buffer, QIODevice::WriteOnly);
    out << quint8(declaredSize);
    out.writeRawData(QByteArray(actualBytes, 'x').constData(), actualBytes);

    QDataStream in(buffer);
    QVariant result = QAmqpFrame::readAmqpField(in, QAmqpMetaType::ShortString);

    if (!truncate) {
        QCOMPARE(result.toString().length(), declaredSize);
    } else {
        // must be rejected cleanly rather than reading garbage/out-of-bounds data
        QVERIFY(!result.isValid() || result.toString().isEmpty());
    }
}

void tst_QAMQPParser::fieldSizeBoundaries_data()
{
    QTest::addColumn<quint32>("declaredSize");
    QTest::addColumn<int>("providedBytes");
    QTest::addColumn<bool>("shouldBeRejected");

    QTest::newRow("zero") << quint32(0) << 0 << false;
    QTest::newRow("small-exact") << quint32(10) << 10 << false;
    QTest::newRow("huge") << quint32(0xFFFFFFF0u) << 16 << true;
    QTest::newRow("frame-max-plus-one") << quint32(AMQP_FRAME_MAX + 1) << 16 << true;
    QTest::newRow("declared-exceeds-available") << quint32(10000) << 16 << true;
}

void tst_QAMQPParser::fieldSizeBoundaries()
{
    QFETCH(quint32, declaredSize);
    QFETCH(int, providedBytes);
    QFETCH(bool, shouldBeRejected);

    QByteArray buffer;
    QDataStream out(&buffer, QIODevice::WriteOnly);
    out << declaredSize;
    out.writeRawData(QByteArray(providedBytes, 'y').constData(), providedBytes);

    QDataStream in(buffer);
    QVariant result = QAmqpFrame::readAmqpField(in, QAmqpMetaType::LongString);

    if (shouldBeRejected) {
        QVERIFY(!result.isValid() || result.toString().isEmpty());
    } else {
        QCOMPARE(quint32(result.toString().toUtf8().size()), declaredSize);
    }
}

void tst_QAMQPParser::utf8StringRoundTrip_data()
{
    QTest::addColumn<QString>("text");

    // non-ASCII rows are built from explicit UTF-8 bytes so the test does not
    // depend on the encoding of this source file
    QTest::newRow("ascii") << QString::fromUtf8("plain-ascii");
    // split literal keeps the hex escape from greedily consuming the following 'e'
    QTest::newRow("latin1-supplement") << QString::fromUtf8("Gr\xC3\xBC\xC3\x9F" "e");
    QTest::newRow("cyrillic") << QString::fromUtf8("\xD0\xBE\xD1\x87\xD0\xB5\xD1\x80\xD0\xB5\xD0\xB4\xD1\x8C");
    QTest::newRow("cjk") << QString::fromUtf8("\xE9\x98\x9F\xE5\x88\x97");
    QTest::newRow("non-bmp") << QString::fromUtf8("queue \xF0\x9F\x9A\x80");
}

void tst_QAMQPParser::utf8StringRoundTrip()
{
    QFETCH(QString, text);

    const QVector<QAmqpMetaType::ValueType> types = {
        QAmqpMetaType::ShortString, QAmqpMetaType::LongString
    };

    foreach (QAmqpMetaType::ValueType type, types) {
        QByteArray buffer;
        QDataStream out(&buffer, QIODevice::WriteOnly);
        QAmqpFrame::writeAmqpField(out, type, text);

        QDataStream in(buffer);
        QCOMPARE(QAmqpFrame::readAmqpField(in, type).toString(), text);
    }
}

void tst_QAMQPParser::truncatedMethodFrame()
{
    // a Method frame body must contain at least methodClass_+id_ (4 bytes); a
    // declared size smaller than that used to underflow arguments_.resize()
    for (qint32 size = 0; size < 4; ++size) {
        QByteArray buffer;
        QDataStream out(&buffer, QIODevice::WriteOnly);
        out << qint8(QAmqpFrame::Method);
        out << quint16(1);
        out << size;
        QByteArray body(size, char(0));
        out.writeRawData(body.constData(), body.size());

        QDataStream in(buffer);
        QAmqpMethodFrame frame;
        in >> frame; // must not crash
        QVERIFY(frame.arguments().isEmpty());
    }
}

void tst_QAMQPParser::deeplyNestedArray()
{
    // nested "array containing an array containing an array ... containing Void";
    // guards against unbounded recursion / stack overflow while decoding tables
    const int depth = 2000;

    QByteArray current;
    {
        QDataStream s(&current, QIODevice::WriteOnly);
        s << qint8('V');
    }

    for (int i = 0; i < depth; ++i) {
        QByteArray next;
        QDataStream s(&next, QIODevice::WriteOnly);
        s << qint8('A');
        s << quint32(current.size());
        s.writeRawData(current.constData(), current.size());
        current = next;
    }

    QByteArray encoded;
    QDataStream out(&encoded, QIODevice::WriteOnly);
    out << quint32(current.size());
    out.writeRawData(current.constData(), current.size());

    QDataStream in(encoded);
    QAmqpTable::readFieldValue(in, QAmqpMetaType::Array); // must not crash
    QCOMPARE(in.status(), QDataStream::ReadCorruptData);
}

void tst_QAMQPParser::messageBodySize_data()
{
    QTest::addColumn<qlonglong>("declaredBodySize");
    QTest::addColumn<bool>("accepted");

    QTest::newRow("empty") << qlonglong(0) << true;
    QTest::newRow("small") << qlonglong(1024) << true;
    QTest::newRow("at-limit") << qlonglong(AMQP_MESSAGE_MAX) << true;
    QTest::newRow("above-limit") << qlonglong(AMQP_MESSAGE_MAX) + 1 << false;
    QTest::newRow("huge") << Q_INT64_C(0x7FFFFFFFFFFF) << false;
    QTest::newRow("negative") << qlonglong(-1) << false;
}

void tst_QAMQPParser::messageBodySize()
{
    QFETCH(qlonglong, declaredBodySize);
    QFETCH(bool, accepted);

    QCOMPARE(QAmqpQueuePrivate::isAcceptableBodySize(declaredBodySize), accepted);
}

void tst_QAMQPParser::messageBodyChunk_data()
{
    QTest::addColumn<qlonglong>("remainingSize");
    QTest::addColumn<qlonglong>("chunkSize");
    QTest::addColumn<bool>("accepted");

    QTest::newRow("exact") << qlonglong(100) << qlonglong(100) << true;
    QTest::newRow("partial") << qlonglong(100) << qlonglong(40) << true;
    QTest::newRow("empty-chunk") << qlonglong(100) << qlonglong(0) << true;
    QTest::newRow("overrun") << qlonglong(100) << qlonglong(101) << false;
    QTest::newRow("overrun-when-complete") << qlonglong(0) << qlonglong(1) << false;
}

void tst_QAMQPParser::messageBodyChunk()
{
    QFETCH(qlonglong, remainingSize);
    QFETCH(qlonglong, chunkSize);
    QFETCH(bool, accepted);

    QCOMPARE(QAmqpQueuePrivate::isAcceptableBodyChunk(remainingSize, chunkSize), accepted);
}

void tst_QAMQPParser::queueBufferLimits_data()
{
    QTest::addColumn<int>("bufferedCount");
    QTest::addColumn<qlonglong>("bufferedBytes");
    QTest::addColumn<qlonglong>("incomingBytes");
    QTest::addColumn<int>("maxMessages");
    QTest::addColumn<qlonglong>("maxBytes");
    QTest::addColumn<qlonglong>("maxMessageSize");
    QTest::addColumn<int>("expected");

    QTest::newRow("exact-limits") << 0 << qlonglong(4) << qlonglong(6) << 1
                                   << qlonglong(10) << qlonglong(6) << int(QAmqpQueue::NoOverflow);
    QTest::newRow("message-count") << 1 << qlonglong(0) << qlonglong(1) << 1
                                    << qlonglong(0) << qlonglong(0)
                                    << int(QAmqpQueue::BufferedMessageCountLimitExceeded);
    QTest::newRow("aggregate-bytes") << 0 << qlonglong(6) << qlonglong(5) << 0
                                      << qlonglong(10) << qlonglong(0)
                                      << int(QAmqpQueue::BufferedByteLimitExceeded);
    QTest::newRow("individual-message") << 0 << qlonglong(0) << qlonglong(11) << 0
                                         << qlonglong(0) << qlonglong(10)
                                         << int(QAmqpQueue::MessageSizeLimitExceeded);
    QTest::newRow("zero-means-unlimited") << 100 << qlonglong(1000) << qlonglong(2000) << 0
                                           << qlonglong(0) << qlonglong(0)
                                           << int(QAmqpQueue::NoOverflow);
}

void tst_QAMQPParser::queueBufferLimits()
{
    QFETCH(int, bufferedCount);
    QFETCH(qlonglong, bufferedBytes);
    QFETCH(qlonglong, incomingBytes);
    QFETCH(int, maxMessages);
    QFETCH(qlonglong, maxBytes);
    QFETCH(qlonglong, maxMessageSize);
    QFETCH(int, expected);

    QCOMPARE(int(QAmqpQueuePrivate::overflowReason(bufferedCount, bufferedBytes, incomingBytes,
                                                  maxMessages, maxBytes, maxMessageSize)), expected);
}

void tst_QAMQPParser::queueBufferLimitConfiguration()
{
    QAmqpClient client;
    QAmqpQueue *queue = client.createQueue("queue-buffer-limit-test");
    QVERIFY(queue);
    QCOMPARE(queue->maxBufferedMessages(), 0);
    QCOMPARE(queue->maxBufferedBytes(), qlonglong(0));
    QCOMPARE(queue->maxMessageSize(), qlonglong(AMQP_MESSAGE_MAX));

    queue->setMaxBufferedMessages(4);
    queue->setMaxBufferedBytes(4096);
    queue->setMaxMessageSize(1024);
    QCOMPARE(queue->maxBufferedMessages(), 4);
    QCOMPARE(queue->maxBufferedBytes(), qlonglong(4096));
    QCOMPARE(queue->maxMessageSize(), qlonglong(1024));
}

static void deliverMessage(QAmqpQueuePrivate &queue, qulonglong deliveryTag)
{
    QAmqpMethodFrame delivery(QAmqpFrame::Basic, QAmqpQueuePrivate::bmDeliver);
    delivery.setChannel(queue.channelNumber);
    QByteArray arguments;
    QDataStream out(&arguments, QIODevice::WriteOnly);
    QAmqpFrame::writeAmqpField(out, QAmqpMetaType::ShortString, QString());
    QAmqpFrame::writeAmqpField(out, QAmqpMetaType::LongLongUint, deliveryTag);
    QAmqpFrame::writeAmqpField(out, QAmqpMetaType::Boolean, false);
    QAmqpFrame::writeAmqpField(out, QAmqpMetaType::ShortString, "exchange");
    QAmqpFrame::writeAmqpField(out, QAmqpMetaType::ShortString, "routing-key");
    delivery.setArguments(arguments);
    queue.deliver(delivery);
}

void tst_QAMQPParser::queueBufferAccountingAndOverflow()
{
    QAmqpClient client;
    QAmqpQueue *publicQueue = client.createQueue("queue-buffer-overflow-test");
    QVERIFY(publicQueue);
    QSignalSpy discarded(publicQueue, &QAmqpQueue::messageDiscarded);

    QAmqpQueuePrivate queue(publicQueue);
    queue.channelNumber = 1;
    queue.maxBufferedMessages = 1;
    queue.maxBufferedBytes = 4;
    queue.maxMessageSize = 4;

    deliverMessage(queue, 1);
    QAmqpContentFrame header(QAmqpFrame::Basic);
    header.setChannel(queue.channelNumber);
    header.setBodySize(4);
    QAmqpContentBodyFrame body;
    body.setChannel(queue.channelNumber);
    body.setBody("data");
    queue._q_content(header);
    queue._q_body(body);

    QCOMPARE(queue.messages.size(), 1);
    QCOMPARE(queue.bufferedBytes, qlonglong(4));
    deliverMessage(queue, 2);
    queue._q_content(header);
    QVERIFY(queue.discardingMessage);
    queue._q_body(body);
    QVERIFY(!queue.discardingMessage);
    QVERIFY(!queue.currentMessage.isValid());
    QCOMPARE(discarded.count(), 1);
    QCOMPARE(discarded.at(0).at(0).toLongLong(), qlonglong(2));
    QCOMPARE(discarded.at(0).at(1).value<QAmqpQueue::OverflowReason>(),
             QAmqpQueue::BufferedMessageCountLimitExceeded);
    QCOMPARE(queue.messages.size(), 1);
    QCOMPARE(queue.bufferedBytes, qlonglong(4));
    QCOMPARE(queue.dequeueMessage().payload(), QByteArray("data"));
    QCOMPARE(queue.bufferedBytes, qlonglong(0));
}

void tst_QAMQPParser::queueProtocolBodyCapEmitsDiscard()
{
    QAmqpClient client;
    QAmqpQueue *publicQueue = client.createQueue("queue-protocol-body-limit-test");
    QVERIFY(publicQueue);
    QSignalSpy discarded(publicQueue, &QAmqpQueue::messageDiscarded);

    QAmqpQueuePrivate queue(publicQueue);
    queue.channelNumber = 1;
    deliverMessage(queue, 17);
    QAmqpContentFrame header(QAmqpFrame::Basic);
    header.setChannel(queue.channelNumber);
    header.setBodySize(qlonglong(AMQP_MESSAGE_MAX) + 1);
    queue._q_content(header);

    QCOMPARE(discarded.count(), 1);
    QCOMPARE(discarded.at(0).at(0).toLongLong(), qlonglong(17));
    QCOMPARE(discarded.at(0).at(1).value<QAmqpQueue::OverflowReason>(),
             QAmqpQueue::MessageSizeLimitExceeded);
    QVERIFY(!queue.currentMessage.isValid());
}

void tst_QAMQPParser::fuzzReadAmqpField()
{
    const QVector<QAmqpMetaType::ValueType> types = {
        QAmqpMetaType::Boolean, QAmqpMetaType::ShortShortUint, QAmqpMetaType::ShortUint,
        QAmqpMetaType::LongUint, QAmqpMetaType::LongLongUint, QAmqpMetaType::ShortString,
        QAmqpMetaType::LongString, QAmqpMetaType::Timestamp, QAmqpMetaType::Hash
    };

    QRandomGenerator rng(0xC0FFEE);
    const int iterations = 1000;
    for (int i = 0; i < iterations; ++i) {
        const QByteArray buffer = randomBytes(rng, rng.bounded(0, 300));

        for (QAmqpMetaType::ValueType type : types) {
            QDataStream in(buffer);
            QVariant result = QAmqpFrame::readAmqpField(in, type);
            if (result.canConvert<QString>())
                QVERIFY(result.toString().toUtf8().size() <= AMQP_FRAME_MAX);
        }
    }
}

void tst_QAMQPParser::fuzzTableFieldValues()
{
    const QVector<QAmqpMetaType::ValueType> types = {
        QAmqpMetaType::ShortShortInt, QAmqpMetaType::ShortInt, QAmqpMetaType::LongInt,
        QAmqpMetaType::LongLongInt, QAmqpMetaType::Float, QAmqpMetaType::Double,
        QAmqpMetaType::Decimal, QAmqpMetaType::Array, QAmqpMetaType::Bytes,
        QAmqpMetaType::Void, QAmqpMetaType::Hash, QAmqpMetaType::LongString
    };

    QRandomGenerator rng(0xBADC0DE);
    const int iterations = 1000;
    for (int i = 0; i < iterations; ++i) {
        const QByteArray buffer = randomBytes(rng, rng.bounded(0, 400));

        for (QAmqpMetaType::ValueType type : types) {
            QDataStream in(buffer);
            QAmqpTable::readFieldValue(in, type); // must not crash/hang
        }
    }
}

void tst_QAMQPParser::fuzzTableDecode()
{
    QRandomGenerator rng(0x5EEDBEEF);
    const int iterations = 1000;
    for (int i = 0; i < iterations; ++i) {
        const QByteArray buffer = randomBytes(rng, rng.bounded(0, 500));

        QDataStream in(buffer);
        QAmqpTable table;
        in >> table; // must not crash/hang
    }
}

void tst_QAMQPParser::fuzzMethodFrame()
{
    QRandomGenerator rng(0xFEEDFACE);
    const int iterations = 1000;
    for (int i = 0; i < iterations; ++i) {
        // deliberately mismatch the declared size against the bytes actually written,
        // exercising both over- and under-declared frame sizes
        const qint32 declaredSize = rng.bounded(0, 400);
        const int actualBytes = rng.bounded(0, 400);

        QByteArray buffer;
        QDataStream out(&buffer, QIODevice::WriteOnly);
        out << qint8(QAmqpFrame::Method);
        out << quint16(1);
        out << declaredSize;
        const QByteArray body = randomBytes(rng, actualBytes);
        out.writeRawData(body.constData(), body.size());

        QDataStream in(buffer);
        QAmqpMethodFrame frame;
        in >> frame; // must not crash/hang regardless of size/actualBytes mismatch
    }
}

QTEST_APPLESS_MAIN(tst_QAMQPParser)
#include "tst_qamqpparser.moc"
