#include <QtTest/QtTest>
#include <QRandomGenerator>
#include <QSslSocket>
#include <QTcpServer>
#include <QTcpSocket>

#include "qamqpframe_p.h"
#include "qamqpclient_p.h"
#include "qamqpclient.h"
#include "qamqpexchange.h"
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
    void shortStringWriteBoundaries_data();
    void shortStringWriteBoundaries();
    void invalidShortStringFramesAreNotWritten();
    void publishRejectsInvalidShortStrings();

    void fieldSizeBoundaries_data();
    void fieldSizeBoundaries();

    void utf8StringRoundTrip_data();
    void utf8StringRoundTrip();

    void truncatedMethodFrame();
    void decodedFrameValidation();
    void truncatedTuneDoesNotMutateNegotiation();
    void incomingMethodSchemas_data();
    void incomingMethodSchemas();
    void invalidMethodValues_data();
    void invalidMethodValues();
    void unsignedTuneValuesRemainValid();
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

void tst_QAMQPParser::shortStringWriteBoundaries_data()
{
    QTest::addColumn<QString>("text");
    QTest::addColumn<bool>("accepted");

    QTest::newRow("ascii-255") << QString(255, QLatin1Char('a')) << true;
    QTest::newRow("ascii-256") << QString(256, QLatin1Char('a')) << false;
    QTest::newRow("utf8-exact-255")
        << (QString(253, QLatin1Char('a')) + QString::fromUtf8("\xC3\xA9")) << true;
    QTest::newRow("utf8-over-255")
        << (QString(254, QLatin1Char('a')) + QString::fromUtf8("\xC3\xA9")) << false;
    QTest::newRow("non-bmp-exact-255")
        << (QString(251, QLatin1Char('a')) + QString::fromUtf8("\xF0\x9F\x9A\x80")) << true;
    QTest::newRow("non-bmp-over-255")
        << (QString(252, QLatin1Char('a')) + QString::fromUtf8("\xF0\x9F\x9A\x80")) << false;
}

void tst_QAMQPParser::shortStringWriteBoundaries()
{
    QFETCH(QString, text);
    QFETCH(bool, accepted);

    QByteArray encoded;
    QDataStream out(&encoded, QIODevice::WriteOnly);
    QCOMPARE(QAmqpFrame::writeAmqpField(out, QAmqpMetaType::ShortString, text), accepted);
    QCOMPARE(out.status() == QDataStream::Ok, accepted);
    if (accepted) {
        QDataStream in(encoded);
        QCOMPARE(QAmqpFrame::readAmqpField(in, QAmqpMetaType::ShortString).toString(), text);
        QCOMPARE(encoded.at(0), char(text.toUtf8().size()));
    } else {
        QVERIFY(encoded.isEmpty());
    }
}

void tst_QAMQPParser::invalidShortStringFramesAreNotWritten()
{
    QByteArray arguments;
    QDataStream argumentsStream(&arguments, QIODevice::WriteOnly);
    QVERIFY(!QAmqpFrame::writeAmqpField(argumentsStream, QAmqpMetaType::ShortString,
                                         QString(256, QLatin1Char('x'))));

    QAmqpMethodFrame method(QAmqpFrame::Basic, 40);
    method.setArguments(arguments, argumentsStream.status());
    QByteArray methodFrame;
    QDataStream methodStream(&methodFrame, QIODevice::WriteOnly);
    methodStream << method;
    QCOMPARE(methodStream.status(), QDataStream::WriteFailed);
    QVERIFY(methodFrame.isEmpty());

    QAmqpContentFrame content(QAmqpFrame::Basic);
    content.setProperty(QAmqpMessage::ContentType, QString(256, QLatin1Char('x')));
    QByteArray contentFrame;
    QDataStream contentStream(&contentFrame, QIODevice::WriteOnly);
    contentStream << content;
    QCOMPARE(contentStream.status(), QDataStream::WriteFailed);
    QVERIFY(contentFrame.isEmpty());
}

void tst_QAMQPParser::publishRejectsInvalidShortStrings()
{
    QAmqpClient client;
    QAmqpExchange *exchange = client.createExchange("valid-exchange");
    QVERIFY(exchange);
    QSignalSpy rejected(exchange, &QAmqpExchange::publishRejected);

    QCOMPARE(exchange->publish(QByteArray("payload"), QString(256, QLatin1Char('r')), "text/plain"),
             QAmqpExchange::RejectedInvalid);
    QCOMPARE(rejected.count(), 1);
    QCOMPARE(rejected.at(0).at(0).value<QAmqpExchange::PublishError>(),
             QAmqpExchange::InvalidShortString);

    QCOMPARE(exchange->publish(QByteArray("payload"), "valid-key", QString(256, QLatin1Char('m'))),
             QAmqpExchange::RejectedInvalid);
    QCOMPARE(rejected.count(), 2);

    QAmqpTable headers;
    headers.insert(QString(256, QLatin1Char('h')), "value");
    QCOMPARE(exchange->publish(QByteArray("payload"), "valid-key", "text/plain", headers),
             QAmqpExchange::RejectedInvalid);
    QCOMPARE(rejected.count(), 3);

    QAmqpMessage::PropertyHash properties;
    properties.insert(QAmqpMessage::CorrelationId, QString(256, QLatin1Char('c')));
    QCOMPARE(exchange->publish(QByteArray("payload"), "valid-key", "text/plain", properties),
             QAmqpExchange::RejectedInvalid);
    QCOMPARE(rejected.count(), 4);
    QCOMPARE(client.bufferedWriteBytes(), qlonglong(0));

    QCOMPARE(exchange->publish(QByteArray("payload"), QString(255, QLatin1Char('r')), "text/plain"),
             QAmqpExchange::Queued);
    QCOMPARE(rejected.count(), 4);
    QVERIFY(client.bufferedWriteBytes() > 0);
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

void tst_QAMQPParser::decodedFrameValidation()
{
    QByteArray validMethodBytes;
    QAmqpMethodFrame validMethod(QAmqpFrame::Basic, QAmqpChannelPrivate::bmQosOk);
    validMethod.setChannel(1);
    QDataStream validMethodOut(&validMethodBytes, QIODevice::WriteOnly);
    validMethodOut << validMethod;
    QDataStream validMethodIn(validMethodBytes);
    QAmqpMethodFrame decodedMethod;
    validMethodIn >> decodedMethod;
    QVERIFY(QAmqpClientPrivate::validateDecodedFrame(decodedMethod, validMethodIn.status(), 4));

    QByteArray truncatedMethodBytes;
    QDataStream truncatedMethodOut(&truncatedMethodBytes, QIODevice::WriteOnly);
    truncatedMethodOut << qint8(QAmqpFrame::Method) << quint16(1) << qint32(3)
                       << qint16(QAmqpFrame::Basic) << qint8(0) << qint8(10)
                       << qint8(QAmqpFrame::FRAME_END);
    QDataStream truncatedMethodIn(truncatedMethodBytes);
    QAmqpMethodFrame truncatedMethod;
    truncatedMethodIn >> truncatedMethod;
    QVERIFY(!QAmqpClientPrivate::validateDecodedFrame(truncatedMethod,
                                                       truncatedMethodIn.status(), 3));

    QByteArray validHeartbeatBytes;
    QAmqpHeartbeatFrame validHeartbeat;
    QDataStream validHeartbeatOut(&validHeartbeatBytes, QIODevice::WriteOnly);
    validHeartbeatOut << validHeartbeat;
    QDataStream validHeartbeatIn(validHeartbeatBytes);
    QAmqpHeartbeatFrame decodedHeartbeat;
    validHeartbeatIn >> decodedHeartbeat;
    QVERIFY(QAmqpClientPrivate::validateDecodedFrame(decodedHeartbeat,
                                                       validHeartbeatIn.status(), 0));

    QByteArray malformedHeartbeatBytes;
    QDataStream malformedHeartbeatOut(&malformedHeartbeatBytes, QIODevice::WriteOnly);
    malformedHeartbeatOut << qint8(QAmqpFrame::Heartbeat) << quint16(0) << qint32(1)
                          << qint8(0) << qint8(QAmqpFrame::FRAME_END);
    QDataStream malformedHeartbeatIn(malformedHeartbeatBytes);
    QAmqpHeartbeatFrame malformedHeartbeat;
    malformedHeartbeatIn >> malformedHeartbeat;
    QVERIFY(!QAmqpClientPrivate::validateDecodedFrame(malformedHeartbeat,
                                                       malformedHeartbeatIn.status(), 1));

    QByteArray malformedContentBytes;
    QDataStream malformedContentOut(&malformedContentBytes, QIODevice::WriteOnly);
    malformedContentOut << qint8(QAmqpFrame::Header) << quint16(1) << qint32(14)
                        << qint16(QAmqpFrame::Basic) << qint16(0) << qlonglong(0)
                        << quint16(QAmqpMessage::ContentType) << qint8(QAmqpFrame::FRAME_END);
    QDataStream malformedContentIn(malformedContentBytes);
    QAmqpContentFrame malformedContent;
    malformedContentIn >> malformedContent;
    QVERIFY(!QAmqpClientPrivate::validateDecodedFrame(malformedContent,
                                                       malformedContentIn.status(), 14));

    QByteArray validContentBytes;
    QDataStream validContentOut(&validContentBytes, QIODevice::WriteOnly);
    validContentOut << qint8(QAmqpFrame::Header) << quint16(1) << qint32(25)
                    << qint16(QAmqpFrame::Basic) << qint16(0) << qlonglong(0)
                    << quint16(QAmqpMessage::Headers) << quint32(7)
                    << quint8(1) << qint8('k') << qint8('I') << qint32(1);
    QDataStream validContentIn(validContentBytes);
    QAmqpContentFrame validContent;
    validContentIn >> validContent;
    QVERIFY(QAmqpClientPrivate::validateDecodedFrame(validContent,
                                                     validContentIn.status(), 25));

    QByteArray trailingContentBytes;
    QDataStream trailingContentOut(&trailingContentBytes, QIODevice::WriteOnly);
    trailingContentOut << qint8(QAmqpFrame::Header) << quint16(1) << qint32(15)
                       << qint16(QAmqpFrame::Basic) << qint16(0) << qlonglong(0)
                       << quint16(0) << quint8(0);
    QDataStream trailingContentIn(trailingContentBytes);
    QAmqpContentFrame trailingContent;
    trailingContentIn >> trailingContent;
    QCOMPARE(trailingContentIn.status(), QDataStream::ReadCorruptData);
    QVERIFY(!QAmqpClientPrivate::validateDecodedFrame(trailingContent,
                                                      trailingContentIn.status(), 15));
}

void tst_QAMQPParser::truncatedTuneDoesNotMutateNegotiation()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    QAmqpClient client;
    QAmqpClientPrivate receiver(&client);
    receiver.socket = new QSslSocket(&client);
    receiver.channelMax = 16;
    QSignalSpy connected(&client, &QAmqpClient::connected);
    receiver.socket->connectToHost(QHostAddress::LocalHost, server.serverPort());
    QVERIFY(receiver.socket->waitForConnected(1000));
    QVERIFY(server.hasPendingConnections() || server.waitForNewConnection(1000));
    QTcpSocket *peer = server.nextPendingConnection();
    QVERIFY(peer);
    QAmqpMethodFrame tune(QAmqpFrame::Connection, QAmqpClientPrivate::miTune);
    QByteArray arguments;
    QDataStream argumentStream(&arguments, QIODevice::WriteOnly);
    argumentStream << quint16(5);
    tune.setArguments(arguments);
    QByteArray wire;
    QDataStream wireStream(&wire, QIODevice::WriteOnly);
    wireStream << tune;
    peer->write(wire);
    peer->flush();
    QVERIFY(receiver.socket->waitForReadyRead(1000));
    receiver._q_readyRead();
    QCOMPARE(receiver.channelMax, quint16(16));
    QTRY_VERIFY(peer->bytesAvailable() > 0);
    QDataStream response(peer->readAll());
    QAmqpMethodFrame close;
    response >> close;
    QCOMPARE(close.methodClass(), QAmqpFrame::Connection);
    QCOMPARE(close.id(), qint16(QAmqpClientPrivate::miClose));
    QDataStream closeArguments(close.arguments());
    quint16 code = 0;
    closeArguments >> code;
    QCOMPARE(code, quint16(QAMQP::FrameError));
    QVERIFY(receiver.closed);
    QAmqpMethodFrame openOk(QAmqpFrame::Connection, QAmqpClientPrivate::miOpenOk);
    openOk.setArguments(QByteArray(1, char(0)));
    QByteArray laterWire;
    QDataStream laterOutput(&laterWire, QIODevice::WriteOnly);
    laterOutput << openOk;
    peer->write(laterWire);
    peer->flush();
    QVERIFY(receiver.socket->waitForReadyRead(1000));
    receiver._q_readyRead();
    QCOMPARE(connected.count(), 0);
    QVERIFY(!receiver.connected);
}

class MethodDispatchProbe : public QAmqpMethodFrameHandler
{
public:
    MethodDispatchProbe() : calls(0) {}
    bool _q_method(const QAmqpMethodFrame &) override { ++calls; return true; }
    int calls;
};

static QByteArray encodedString(const QString &text, QAmqpMetaType::ValueType type)
{
    QByteArray bytes;
    QDataStream stream(&bytes, QIODevice::WriteOnly);
    QAmqpFrame::writeAmqpField(stream, type, text);
    return bytes;
}

void tst_QAMQPParser::incomingMethodSchemas_data()
{
    QTest::addColumn<int>("methodClass");
    QTest::addColumn<int>("methodId");
    QTest::addColumn<QByteArray>("arguments");
    const QByteArray shortEmpty = encodedString(QString(), QAmqpMetaType::ShortString);
    const QByteArray longEmpty = encodedString(QString(), QAmqpMetaType::LongString);
    const QByteArray consumer = encodedString("consumer", QAmqpMetaType::ShortString);
    QByteArray close;
    QDataStream closeStream(&close, QIODevice::WriteOnly);
    closeStream << quint16(200);
    QAmqpFrame::writeAmqpField(closeStream, QAmqpMetaType::ShortString, "closed");
    closeStream << quint16(0) << quint16(0);
    QByteArray start;
    QDataStream startStream(&start, QIODevice::WriteOnly);
    startStream << quint8(0) << quint8(9) << QAmqpTable();
    QAmqpFrame::writeAmqpField(startStream, QAmqpMetaType::LongString, "PLAIN");
    QAmqpFrame::writeAmqpField(startStream, QAmqpMetaType::LongString, "en_US");
    QByteArray tune;
    QDataStream tuneStream(&tune, QIODevice::WriteOnly);
    tuneStream << quint16(16) << quint32(4096) << quint16(30);
    QByteArray count;
    QDataStream countStream(&count, QIODevice::WriteOnly);
    countStream << quint32(1);
    QByteArray deliveryTag;
    QDataStream tagStream(&deliveryTag, QIODevice::WriteOnly);
    tagStream << qulonglong(1);
    const QByteArray delivery = deliveryTag + QByteArray(1, char(0)) + shortEmpty + shortEmpty;
    QByteArray returned;
    QDataStream returnStream(&returned, QIODevice::WriteOnly);
    returnStream << quint16(312);
    returned += shortEmpty + shortEmpty + shortEmpty;
    const auto add = [](const char *name, int methodClass, int methodId, const QByteArray &arguments) {
        QTest::newRow(name) << methodClass << methodId << arguments;
    };
    add("connection-start", QAmqpFrame::Connection, 10, start);
    add("connection-secure", QAmqpFrame::Connection, 20, longEmpty);
    add("connection-tune", QAmqpFrame::Connection, 30, tune);
    QByteArray unlimitedTune;
    QDataStream unlimitedStream(&unlimitedTune, QIODevice::WriteOnly);
    unlimitedStream << quint16(0) << quint32(0) << quint16(0);
    add("connection-tune-unlimited", QAmqpFrame::Connection, 30, unlimitedTune);
    QByteArray maximumTune;
    QDataStream maximumStream(&maximumTune, QIODevice::WriteOnly);
    maximumStream << quint16(65535) << quint32(0xffffffffu) << quint16(65535);
    add("connection-tune-unsigned-maximum", QAmqpFrame::Connection, 30, maximumTune);
    add("connection-open-ok", QAmqpFrame::Connection, 41, shortEmpty);
    add("connection-close", QAmqpFrame::Connection, 50, close);
    add("connection-close-ok", QAmqpFrame::Connection, 51, QByteArray());
    add("channel-open-ok", QAmqpFrame::Channel, 11, longEmpty);
    add("channel-flow", QAmqpFrame::Channel, 20, QByteArray(1, char(1)));
    add("channel-flow-ok", QAmqpFrame::Channel, 21, QByteArray(1, char(0)));
    add("channel-close", QAmqpFrame::Channel, 40, close);
    add("channel-close-ok", QAmqpFrame::Channel, 41, QByteArray());
    add("exchange-declare-ok", QAmqpFrame::Exchange, 11, QByteArray());
    add("exchange-delete-ok", QAmqpFrame::Exchange, 21, QByteArray());
    add("queue-declare-ok", QAmqpFrame::Queue, 11, consumer + count + count);
    add("queue-bind-ok", QAmqpFrame::Queue, 21, QByteArray());
    add("queue-purge-ok", QAmqpFrame::Queue, 31, count);
    add("queue-delete-ok", QAmqpFrame::Queue, 41, count);
    add("queue-unbind-ok", QAmqpFrame::Queue, 51, QByteArray());
    add("basic-qos-ok", QAmqpFrame::Basic, 11, QByteArray());
    add("basic-consume-ok", QAmqpFrame::Basic, 21, consumer);
    add("basic-cancel-ok", QAmqpFrame::Basic, 31, consumer);
    add("basic-return", QAmqpFrame::Basic, 50, returned);
    add("basic-deliver", QAmqpFrame::Basic, 60, consumer + delivery);
    add("basic-get-ok", QAmqpFrame::Basic, 71, delivery + count);
    add("basic-get-empty", QAmqpFrame::Basic, 72, shortEmpty);
    add("basic-ack", QAmqpFrame::Basic, 80, deliveryTag + QByteArray(1, char(1)));
    add("basic-nack", QAmqpFrame::Basic, 120, deliveryTag + QByteArray(1, char(3)));
    add("basic-recover-ok", QAmqpFrame::Basic, 111, QByteArray());
    add("confirm-select-ok", QAmqpFrame::Confirm, 11, QByteArray());
    add("tx-select-ok", QAmqpFrame::Tx, 11, QByteArray());
    add("tx-commit-ok", QAmqpFrame::Tx, 21, QByteArray());
    add("tx-rollback-ok", QAmqpFrame::Tx, 31, QByteArray());
}

void tst_QAMQPParser::incomingMethodSchemas()
{
    QFETCH(int, methodClass);
    QFETCH(int, methodId);
    QFETCH(QByteArray, arguments);
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QAmqpMethodFrame method(static_cast<QAmqpFrame::MethodClass>(methodClass), qint16(methodId));
    method.setChannel(methodClass == QAmqpFrame::Connection ? 0 : 1);
    method.setArguments(arguments);
    QVERIFY(QAmqpClientPrivate::validateDecodedFrame(method, QDataStream::Ok, arguments.size() + 4));

    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    QAmqpClient client;
    QAmqpClientPrivate receiver(&client);
    receiver.socket = new QSslSocket(&client);
    receiver.channelMax = 16;
    receiver.heartbeatDelay = 30;
    const qint32 originalFrameMax = receiver.frameMax;
    MethodDispatchProbe probe;
    receiver.methodHandlersByChannel[1].append(&probe);
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy disconnected(&client, &QAmqpClient::disconnected);
    receiver.socket->connectToHost(QHostAddress::LocalHost, server.serverPort());
    QVERIFY(receiver.socket->waitForConnected(1000));
    QVERIFY(server.hasPendingConnections() || server.waitForNewConnection(1000));
    QTcpSocket *peer = server.nextPendingConnection();
    QVERIFY(peer);
    for (int length = 0; length <= arguments.size(); ++length) {
        receiver.closed = false;
        method.setArguments(length == arguments.size() ? arguments + QByteArray(1, 'x') : arguments.left(length));
        QByteArray wire;
        QDataStream output(&wire, QIODevice::WriteOnly);
        output << method;
        peer->write(wire);
        peer->flush();
        QVERIFY(receiver.socket->waitForReadyRead(1000));
        receiver._q_readyRead();
        QCOMPARE(probe.calls, 0);
        QCOMPARE(connected.count(), 0);
        QCOMPARE(disconnected.count(), 0);
        QCOMPARE(receiver.channelMax, quint16(16));
        QCOMPARE(receiver.heartbeatDelay, quint16(30));
        QCOMPARE(receiver.frameMax, originalFrameMax);
        QVERIFY(!receiver.connected);
        QTRY_VERIFY(peer->bytesAvailable() > 0);
        QDataStream response(peer->readAll());
        QAmqpMethodFrame close;
        response >> close;
        QCOMPARE(close.methodClass(), QAmqpFrame::Connection);
        QCOMPARE(close.id(), qint16(QAmqpClientPrivate::miClose));
        QDataStream closeArguments(close.arguments());
        quint16 code = 0;
        closeArguments >> code;
        QCOMPARE(code, quint16(QAMQP::FrameError));
    }
}

void tst_QAMQPParser::invalidMethodValues_data()
{
    QTest::addColumn<int>("methodClass");
    QTest::addColumn<int>("methodId");
    QTest::addColumn<QByteArray>("arguments");
    const auto add = [](const char *name, int methodClass, int methodId, const QByteArray &arguments) {
        QTest::newRow(name) << methodClass << methodId << arguments;
    };
    QByteArray tune;
    QDataStream tuneStream(&tune, QIODevice::WriteOnly);
    tuneStream << quint16(16) << quint32(4095) << quint16(30);
    add("tune-below-minimum", QAmqpFrame::Connection, 30, tune);
    add("flow-invalid-bits", QAmqpFrame::Channel, 20, QByteArray(1, char(2)));
    add("flow-ok-invalid-bits", QAmqpFrame::Channel, 21, QByteArray(1, char(0x80)));
    add("empty-consumer", QAmqpFrame::Basic, 21, QByteArray(1, char(0)));
    add("empty-cancel-tag", QAmqpFrame::Basic, 31, QByteArray(1, char(0)));
    add("short-string-overrun", QAmqpFrame::Basic, 21, QByteArray::fromHex("0561"));
    add("long-string-overrun", QAmqpFrame::Connection, 20, QByteArray::fromHex("0000000561"));
    add("ack-zero-single-tag", QAmqpFrame::Basic, 80, QByteArray(9, char(0)));
    add("ack-invalid-bits", QAmqpFrame::Basic, 80, QByteArray::fromHex("000000000000000102"));
    add("nack-invalid-bits", QAmqpFrame::Basic, 120, QByteArray::fromHex("000000000000000104"));
    QByteArray start;
    QDataStream startStream(&start, QIODevice::WriteOnly);
    startStream << quint8(0) << quint8(9) << quint32(5) << quint8(1) << qint8('k') << qint8('?');
    add("malformed-start-table", QAmqpFrame::Connection, 10, start);
    QByteArray unknownField;
    QDataStream unknownStream(&unknownField, QIODevice::WriteOnly);
    unknownStream << quint8(0) << quint8(9) << quint32(3) << quint8(1) << qint8('k') << qint8('?');
    unknownField += encodedString("PLAIN", QAmqpMetaType::LongString) +
        encodedString("en_US", QAmqpMetaType::LongString);
    add("unknown-start-table-field", QAmqpFrame::Connection, 10, unknownField);
    QByteArray badVersion;
    QDataStream versionStream(&badVersion, QIODevice::WriteOnly);
    versionStream << quint8(1) << quint8(9) << QAmqpTable();
    QAmqpFrame::writeAmqpField(versionStream, QAmqpMetaType::LongString, "PLAIN");
    QAmqpFrame::writeAmqpField(versionStream, QAmqpMetaType::LongString, "en_US");
    add("unsupported-start-version", QAmqpFrame::Connection, 10, badVersion);
    add("unsupported-method", QAmqpFrame::Connection, 99, QByteArray());
    add("client-only-method", QAmqpFrame::Basic, 40, QByteArray());
}

void tst_QAMQPParser::invalidMethodValues()
{
    QFETCH(int, methodClass);
    QFETCH(int, methodId);
    QFETCH(QByteArray, arguments);
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    QAmqpClient client;
    QAmqpClientPrivate receiver(&client);
    receiver.socket = new QSslSocket(&client);
    receiver.channelMax = 16;
    const qint32 originalFrameMax = receiver.frameMax;
    MethodDispatchProbe probe;
    receiver.methodHandlersByChannel[1].append(&probe);
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy disconnected(&client, &QAmqpClient::disconnected);
    receiver.socket->connectToHost(QHostAddress::LocalHost, server.serverPort());
    QVERIFY(receiver.socket->waitForConnected(1000));
    QVERIFY(server.hasPendingConnections() || server.waitForNewConnection(1000));
    QTcpSocket *peer = server.nextPendingConnection();
    QVERIFY(peer);
    QAmqpMethodFrame method(static_cast<QAmqpFrame::MethodClass>(methodClass), qint16(methodId));
    method.setChannel(methodClass == QAmqpFrame::Connection ? 0 : 1);
    method.setArguments(arguments);
    QVERIFY(!QAmqpClientPrivate::validateMethodArguments(method));
    QByteArray wire;
    QDataStream output(&wire, QIODevice::WriteOnly);
    output << method;
    peer->write(wire);
    peer->flush();
    QVERIFY(receiver.socket->waitForReadyRead(1000));
    receiver._q_readyRead();
    QCOMPARE(probe.calls, 0);
    QCOMPARE(connected.count(), 0);
    QCOMPARE(disconnected.count(), 0);
    QCOMPARE(receiver.channelMax, quint16(16));
    QCOMPARE(receiver.frameMax, originalFrameMax);
    QVERIFY(!receiver.connected);
    QTRY_VERIFY(peer->bytesAvailable() > 0);
    QDataStream response(peer->readAll());
    QAmqpMethodFrame close;
    response >> close;
    QCOMPARE(close.methodClass(), QAmqpFrame::Connection);
    QCOMPARE(close.id(), qint16(QAmqpClientPrivate::miClose));
    QDataStream closeArguments(close.arguments());
    quint16 code = 0;
    closeArguments >> code;
    QCOMPARE(code, quint16(QAMQP::FrameError));
}

void tst_QAMQPParser::unsignedTuneValuesRemainValid()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    QAmqpClient client;
    QAmqpClientPrivate receiver(&client);
    receiver.socket = new QSslSocket(&client);
    receiver.socket->connectToHost(QHostAddress::LocalHost, server.serverPort());
    QVERIFY(receiver.socket->waitForConnected(1000));
    QVERIFY(server.hasPendingConnections() || server.waitForNewConnection(1000));
    QTcpSocket *peer = server.nextPendingConnection();
    QVERIFY(peer);
    QAmqpMethodFrame tune(QAmqpFrame::Connection, QAmqpClientPrivate::miTune);
    QByteArray arguments;
    QDataStream argumentStream(&arguments, QIODevice::WriteOnly);
    argumentStream << quint16(65535) << quint32(0xffffffffu) << quint16(65535);
    tune.setArguments(arguments);
    QByteArray wire;
    QDataStream output(&wire, QIODevice::WriteOnly);
    output << tune;
    peer->write(wire);
    peer->flush();
    QVERIFY(receiver.socket->waitForReadyRead(1000));
    receiver._q_readyRead();
    QCOMPARE(receiver.channelMax, quint16(65535));
    QCOMPARE(receiver.heartbeatDelay, quint16(65535));
    QCOMPARE(receiver.frameMax, qint32(AMQP_FRAME_MAX));
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
