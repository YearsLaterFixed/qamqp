#include <QtTest/QtTest>
#include <QTcpServer>
#include <QTcpSocket>
#include <QSslSocket>
#include <QTimer>

#include "qamqpchannel.h"
#include "qamqpchannel_p.h"
#include "qamqpclient.h"
#include "qamqpclient_p.h"
#include "qamqpexchange_p.h"

class TestChannel : public QAmqpChannel
{
    Q_OBJECT
public:
    TestChannel()
        : QAmqpChannel(new QAmqpChannelPrivate(this), 0)
    {
        Q_D(QAmqpChannel);
        d->channelNumber = 1;
    }

    void receiveMethod(const QAmqpMethodFrame &frame)
    {
        Q_D(QAmqpChannel);
        d->_q_method(frame);
    }

    void resetState()
    {
        Q_D(QAmqpChannel);
        d->resetInternalState();
    }

protected:
    virtual void channelOpened() {}
    virtual void channelClosed() {}
};

class tst_QAMQPFlow : public QObject
{
    Q_OBJECT
private Q_SLOTS:
    void serverInitiatedFlow();
    void publishDeferral_data();
    void publishDeferral();
    void pendingPublishAdmission();
    void writeQueueReservation();
    void asynchronousWriteQueue();
    void controlFramesHaveIndependentBudget();
    void controlFramesRespectPublishSequence();
    void partialControlFrameCannotBeInterrupted();
    void controlReserveExhaustionIsObservable();
    void controlWriteTimeoutIsObservable();
    void saturatedBudgetAllowsStartupAndReconnect();
    void writeQueueDiscardOnDisconnect();
    void flowStateResetOnReconnect();
};

void tst_QAMQPFlow::serverInitiatedFlow()
{
    TestChannel channel;
    QSignalSpy paused(&channel, SIGNAL(paused()));
    QSignalSpy resumed(&channel, SIGNAL(resumed()));

    QAmqpMethodFrame pause(QAmqpFrame::Channel, QAmqpChannelPrivate::miFlow);
    pause.setChannel(channel.channelNumber());
    QByteArray pauseArguments;
    QDataStream pauseStream(&pauseArguments, QIODevice::WriteOnly);
    QAmqpFrame::writeAmqpField(pauseStream, QAmqpMetaType::ShortShortUint, 0);
    pause.setArguments(pauseArguments);
    channel.receiveMethod(pause);

    QVERIFY(!channel.isFlowActive());
    QCOMPARE(paused.count(), 1);
    QCOMPARE(resumed.count(), 0);

    QAmqpMethodFrame acknowledgement = QAmqpChannelPrivate::flowOkFrame(channel.channelNumber(), false);
    QCOMPARE(acknowledgement.methodClass(), QAmqpFrame::Channel);
    QCOMPARE(acknowledgement.id(), qint16(QAmqpChannelPrivate::miFlowOk));
    QCOMPARE(acknowledgement.channel(), quint16(channel.channelNumber()));
    QByteArray acknowledgementArguments = acknowledgement.arguments();
    QDataStream acknowledgementStream(&acknowledgementArguments, QIODevice::ReadOnly);
    QCOMPARE(QAmqpFrame::readAmqpField(acknowledgementStream, QAmqpMetaType::Boolean).toBool(), false);

    QAmqpMethodFrame resume(QAmqpFrame::Channel, QAmqpChannelPrivate::miFlow);
    resume.setChannel(channel.channelNumber());
    QByteArray resumeArguments;
    QDataStream resumeStream(&resumeArguments, QIODevice::WriteOnly);
    QAmqpFrame::writeAmqpField(resumeStream, QAmqpMetaType::ShortShortUint, 1);
    resume.setArguments(resumeArguments);
    channel.receiveMethod(resume);

    QVERIFY(channel.isFlowActive());
    QCOMPARE(paused.count(), 1);
    QCOMPARE(resumed.count(), 1);
}

static QAmqpMethodFrame flowFrame(quint16 channelNumber, bool active)
{
    QAmqpMethodFrame frame(QAmqpFrame::Channel, QAmqpChannelPrivate::miFlow);
    frame.setChannel(channelNumber);

    QByteArray arguments;
    QDataStream stream(&arguments, QIODevice::WriteOnly);
    QAmqpFrame::writeAmqpField(stream, QAmqpMetaType::ShortShortUint, active ? 1 : 0);
    frame.setArguments(arguments);
    return frame;
}

void tst_QAMQPFlow::publishDeferral_data()
{
    QTest::addColumn<bool>("channelOpened");
    QTest::addColumn<bool>("flowActive");
    QTest::addColumn<bool>("deferred");

    QTest::newRow("open-and-active") << true << true << false;
    QTest::newRow("open-but-paused") << true << false << true;
    QTest::newRow("not-open-active") << false << true << true;
    QTest::newRow("not-open-paused") << false << false << true;
}

void tst_QAMQPFlow::publishDeferral()
{
    QFETCH(bool, channelOpened);
    QFETCH(bool, flowActive);
    QFETCH(bool, deferred);

    QCOMPARE(QAmqpExchangePrivate::shouldDeferPublish(channelOpened, flowActive), deferred);
}

void tst_QAMQPFlow::pendingPublishAdmission()
{
    QAmqpClient client;
    QAmqpExchange *exchange = client.createExchange("pending-publish-test");
    QVERIFY(exchange);

    for (int i = 0; i < 1000; ++i) {
        QCOMPARE(exchange->publish(QByteArray("m"), "routing-key", "text/plain"),
                 QAmqpExchange::Queued);
    }
    QCOMPARE(exchange->publish(QByteArray("overflow"), "routing-key", "text/plain"),
             QAmqpExchange::RejectedLimit);

    QAmqpClient smallBudgetClient;
    smallBudgetClient.setMaxWriteQueueBytes(1);
    QAmqpExchange *oversized = smallBudgetClient.createExchange("small-write-budget");
    QVERIFY(oversized);
    QCOMPARE(oversized->publish(QByteArray("m"), "routing-key", "text/plain"),
             QAmqpExchange::RejectedLimit);

    QAmqpClient sharedBudgetClient;
    QAmqpExchange *first = sharedBudgetClient.createExchange("shared-budget-first");
    QAmqpExchange *second = sharedBudgetClient.createExchange("shared-budget-second");
    QVERIFY(first);
    QVERIFY(second);
    QCOMPARE(first->publish(QByteArray("m"), "routing-key", "text/plain"),
             QAmqpExchange::Queued);
    const qlonglong firstReservation = sharedBudgetClient.bufferedWriteBytes();
    QVERIFY(firstReservation > 0);
    sharedBudgetClient.setMaxWriteQueueBytes(firstReservation);
    QCOMPARE(second->publish(QByteArray("m"), "routing-key", "text/plain"),
             QAmqpExchange::RejectedLimit);
    QCOMPARE(sharedBudgetClient.bufferedWriteBytes(), firstReservation);
}

void tst_QAMQPFlow::writeQueueReservation()
{
    QCOMPARE(QAmqpClientPrivate::canReserveWriteBytes(0, 0, 10), true);
    QCOMPARE(QAmqpClientPrivate::canReserveWriteBytes(6, 4, 10), true);
    QCOMPARE(QAmqpClientPrivate::canReserveWriteBytes(6, 5, 10), false);
    QCOMPARE(QAmqpClientPrivate::canReserveWriteBytes(11, 0, 10), false);
    QCOMPARE(QAmqpClientPrivate::canReserveWriteBytes(0, -1, 10), false);
    QCOMPARE(QAmqpClientPrivate::canReserveWriteBytes(0, 1, 0), false);
}

void tst_QAMQPFlow::asynchronousWriteQueue()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));

    QAmqpClient client;
    QAmqpClientPrivate writer(&client);
    writer.socket = new QSslSocket(&client);
    writer.maxWriteQueueBytes = 3;
    QObject::connect(writer.socket, &QIODevice::bytesWritten, &client,
                     [&writer](qint64 bytes) { writer._q_bytesWritten(bytes); });
    QSignalSpy overflow(&client, &QAmqpClient::writeQueueOverflow);
    QSignalSpy connected(writer.socket, &QAbstractSocket::connected);

    writer.socket->connectToHost(QHostAddress::LocalHost, server.serverPort());
    QVERIFY(connected.wait(1000));
    QTRY_VERIFY(server.hasPendingConnections());
    QTcpSocket *peer = server.nextPendingConnection();
    QVERIFY(peer);

    QVERIFY(!writer.sendFrameBatch(QByteArray("four")));
    QCOMPARE(overflow.count(), 1);
    QCOMPARE(writer.bufferedWriteBytes, qint64(0));

    QVERIFY(writer.sendFrameBatch(QByteArray("abc")));
    QTRY_COMPARE(peer->bytesAvailable(), qint64(3));
    QCOMPARE(peer->readAll(), QByteArray("abc"));
    QTRY_COMPARE(writer.bufferedWriteBytes, qint64(0));
}

void tst_QAMQPFlow::controlFramesHaveIndependentBudget()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));

    QAmqpClient client;
    QAmqpClientPrivate writer(&client);
    writer.socket = new QSslSocket(&client);
    writer.maxWriteQueueBytes = 1;
    QObject::connect(writer.socket, &QIODevice::bytesWritten, &client,
                     [&writer](qint64 bytes) { writer._q_bytesWritten(bytes); });
    QSignalSpy connected(writer.socket, &QAbstractSocket::connected);
    writer.socket->connectToHost(QHostAddress::LocalHost, server.serverPort());
    QVERIFY(connected.wait(1000));
    QTRY_VERIFY(server.hasPendingConnections());
    QTcpSocket *peer = server.nextPendingConnection();
    QVERIFY(peer);

    QVERIFY(writer.reserveWriteBytes(1));
    QAmqpHeartbeatFrame heartbeat;
    QVERIFY(writer.sendFrame(heartbeat));
    QTRY_COMPARE(peer->bytesAvailable(), qint64(8));
    QCOMPARE(peer->readAll().toHex(), QByteArray("08000000000000ce"));
    QCOMPARE(writer.bufferedWriteBytes, qint64(1));
    QVERIFY(!writer.sendFrameBatch(QByteArray("x")));

    writer.releaseWriteBytes(1);
    QVERIFY(writer.sendFrameBatch(QByteArray("x")));
    QTRY_COMPARE(peer->bytesAvailable(), qint64(1));
    QCOMPARE(peer->readAll(), QByteArray("x"));
    QTRY_COMPARE(writer.bufferedWriteBytes, qint64(0));
}

class PartialWriteSocket : public QSslSocket
{
public:
    explicit PartialWriteSocket(QObject *parent) : QSslSocket(parent), maximumWriteSize(4093) {}
    qint64 maximumWriteSize;

protected:
    qint64 writeData(const char *data, qint64 bytes) override
    {
        return maximumWriteSize ? QSslSocket::writeData(data, qMin(bytes, maximumWriteSize)) : 0;
    }
};

class ControlTestClient : public QAmqpClient
{
public:
    QAmqpClientPrivate *state() { return d_func(); }
};

static QTcpSocket *connectControlWriter(ControlTestClient &client, QTcpServer &server)
{
    if (!server.isListening() && !server.listen(QHostAddress::LocalHost, 0))
        return nullptr;
    QAmqpClientPrivate *writer = client.state();
    delete writer->socket;
    writer->socket = new PartialWriteSocket(&client);
    QObject::connect(writer->socket, &QIODevice::bytesWritten, &client,
                     [writer](qint64 bytes) { writer->_q_bytesWritten(bytes); });
    QObject::connect(writer->socket, &QAbstractSocket::disconnected, &client,
                     [writer] { writer->_q_socketDisconnected(); });
    writer->socket->connectToHost(QHostAddress::LocalHost, server.serverPort());
    if (!writer->socket->waitForConnected(1000))
        return nullptr;
    if (!server.hasPendingConnections() && !server.waitForNewConnection(1000))
        return nullptr;
    return server.nextPendingConnection();
}

static QByteArray encodeFrame(const QAmqpFrame &frame)
{
    QByteArray bytes;
    QDataStream stream(&bytes, QIODevice::WriteOnly);
    stream << frame;
    return bytes;
}

void tst_QAMQPFlow::controlFramesRespectPublishSequence()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);

    QAmqpMethodFrame publish(QAmqpFrame::Basic, 40);
    publish.setChannel(1);
    QByteArray arguments;
    QDataStream publishArguments(&arguments, QIODevice::WriteOnly);
    publishArguments << quint16(0);
    QAmqpFrame::writeAmqpField(publishArguments, QAmqpMetaType::ShortString, "exchange");
    QAmqpFrame::writeAmqpField(publishArguments, QAmqpMetaType::ShortString, "route");
    publishArguments << quint8(0);
    publish.setArguments(arguments);
    QAmqpContentFrame content(QAmqpFrame::Basic);
    content.setChannel(1);
    content.setBodySize(71 * 1024);
    QAmqpContentBodyFrame largeBody;
    largeBody.setChannel(1);
    largeBody.setBody(QByteArray(70 * 1024, 'a'));
    QAmqpContentBodyFrame smallBody;
    smallBody.setChannel(1);
    smallBody.setBody(QByteArray(1024, 'b'));
    const QByteArray prefix = encodeFrame(publish) + encodeFrame(content) + encodeFrame(largeBody);
    const QByteArray suffix = encodeFrame(smallBody);
    QVERIFY(writer.sendFrameBatch(prefix + suffix));
    QVERIFY(!writer.pendingWrites.isEmpty());
    QVERIFY(writer.pendingWrites.head().offset < writer.pendingWrites.head().frameEnd);

    const QAmqpMethodFrame sameChannel = QAmqpChannelPrivate::flowOkFrame(1, true);
    const QAmqpMethodFrame otherChannel = QAmqpChannelPrivate::flowOkFrame(2, true);
    QAmqpHeartbeatFrame heartbeat;
    QVERIFY(writer.sendFrame(sameChannel));
    QVERIFY(writer.sendFrame(otherChannel));
    QVERIFY(writer.sendFrame(heartbeat));
    QVERIFY(writer.sendFrameBatch(prefix + suffix));
    const QByteArray expected = prefix + encodeFrame(otherChannel) + encodeFrame(heartbeat) +
        suffix + encodeFrame(sameChannel) + prefix + suffix;
    bool eventLoopServiced = false;
    QTimer::singleShot(0, &client, [&eventLoopServiced] { eventLoopServiced = true; });
    QTRY_COMPARE(peer->bytesAvailable(), qint64(expected.size()));
    QCOMPARE(peer->readAll(), expected);
    QVERIFY(eventLoopServiced);
    QTRY_COMPARE(writer.bufferedWriteBytes, qint64(0));
    QTRY_COMPARE(writer.bufferedControlBytes, qint64(0));
}

void tst_QAMQPFlow::partialControlFrameCannotBeInterrupted()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);

    QAmqpContentBodyFrame firstBody;
    firstBody.setChannel(1);
    firstBody.setBody(QByteArray(60 * 1024, 'a'));
    QAmqpMethodFrame control(QAmqpFrame::Connection, QAmqpClientPrivate::miStartOk);
    control.setArguments(QByteArray(16 * 1024, 'c'));
    QAmqpContentBodyFrame secondBody;
    secondBody.setChannel(2);
    secondBody.setBody(QByteArray(1024, 'b'));
    QAmqpHeartbeatFrame heartbeat;
    QVERIFY(writer.sendFrameBatch(encodeFrame(firstBody)));
    QVERIFY(writer.sendFrame(control));
    QVERIFY(!writer.controlWrites.isEmpty());
    QVERIFY(writer.controlWrites.head().offset > 0);
    QVERIFY(writer.controlWrites.head().offset < writer.controlWrites.head().data.size());
    QVERIFY(writer.sendFrameBatch(encodeFrame(secondBody)));
    QVERIFY(writer.sendFrame(heartbeat));
    const QByteArray expected = encodeFrame(firstBody) + encodeFrame(control) +
        encodeFrame(heartbeat) + encodeFrame(secondBody);
    QTRY_COMPARE(peer->bytesAvailable(), qint64(expected.size()));
    QCOMPARE(peer->readAll(), expected);
    QTRY_COMPARE(writer.bufferedWriteBytes, qint64(0));
    QTRY_COMPARE(writer.bufferedControlBytes, qint64(0));
}

void tst_QAMQPFlow::controlReserveExhaustionIsObservable()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QVERIFY(connectControlWriter(client, server));
    client.setAutoReconnect(true);
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    QAmqpMethodFrame control(QAmqpFrame::Connection, QAmqpClientPrivate::miStartOk);
    control.setArguments(QByteArray(QAmqpClientPrivate::ControlWriteQueueBytes - 12, 'c'));
    QVERIFY(writer.sendFrame(control));
    QCOMPARE(writer.bufferedControlBytes, qint64(QAmqpClientPrivate::ControlWriteQueueBytes));
    QAmqpHeartbeatFrame heartbeat;
    QVERIFY(!writer.sendFrame(heartbeat));
    QCOMPARE(errors.count(), 1);
    QCOMPARE(client.error(), QAMQP::ResourceError);
    QCOMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
    QCOMPARE(writer.bufferedControlBytes, qint64(0));
    QVERIFY(writer.reconnectTimer->isActive());
    writer.reconnectTimer->stop();
}

void tst_QAMQPFlow::controlWriteTimeoutIsObservable()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QVERIFY(connectControlWriter(client, server));
    static_cast<PartialWriteSocket *>(writer.socket)->maximumWriteSize = 0;
    client.setAutoReconnect(true);
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    QAmqpHeartbeatFrame heartbeat;
    QVERIFY(writer.sendFrame(heartbeat));
    writer.controlWriteTimer->start(1);
    QTRY_COMPARE(errors.count(), 1);
    QCOMPARE(client.error(), QAMQP::ResourceError);
    QCOMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
    QCOMPARE(writer.bufferedControlBytes, qint64(0));
    QVERIFY(writer.reconnectTimer->isActive());
    writer.reconnectTimer->stop();
}

void tst_QAMQPFlow::saturatedBudgetAllowsStartupAndReconnect()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    writer.maxWriteQueueBytes = 1;
    QVERIFY(writer.reserveWriteBytes(1));
    for (int attempt = 0; attempt < 2; ++attempt) {
        QTcpSocket *peer = connectControlWriter(client, server);
        QVERIFY(peer);
        writer._q_socketConnected();
        writer.tuneOk();
        writer.open();
        QAmqpMethodFrame channelOpen(QAmqpFrame::Channel, QAmqpChannelPrivate::miOpen);
        channelOpen.setChannel(1);
        channelOpen.setArguments(QByteArray(1, char(0)));
        QVERIFY(writer.sendFrame(channelOpen));
        QAmqpHeartbeatFrame heartbeat;
        QVERIFY(writer.sendFrame(heartbeat));

        QAmqpMethodFrame tuneOk(QAmqpFrame::Connection, QAmqpClientPrivate::miTuneOk);
        QByteArray tuneArguments;
        QDataStream tuneStream(&tuneArguments, QIODevice::WriteOnly);
        tuneStream << quint16(writer.channelMax) << qint32(writer.frameMax)
                   << quint16(writer.heartbeatDelay);
        tuneOk.setArguments(tuneArguments);
        QAmqpMethodFrame connectionOpen(QAmqpFrame::Connection, QAmqpClientPrivate::miOpen);
        QByteArray openArguments;
        QDataStream openStream(&openArguments, QIODevice::WriteOnly);
        QAmqpFrame::writeAmqpField(openStream, QAmqpMetaType::ShortString, writer.virtualHost);
        openStream << quint8(0) << quint8(0);
        connectionOpen.setArguments(openArguments);
        const QByteArray expected = QByteArray("AMQP\0\0\x09\x01", 8) + encodeFrame(tuneOk) +
            encodeFrame(connectionOpen) + encodeFrame(channelOpen) + encodeFrame(heartbeat);
        QTRY_COMPARE(peer->bytesAvailable(), qint64(expected.size()));
        QCOMPARE(peer->readAll(), expected);
        QTRY_COMPARE(writer.bufferedControlBytes, qint64(0));
        QCOMPARE(writer.bufferedWriteBytes, qint64(1));
        peer->abort();
        QTRY_COMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
        QCOMPARE(writer.bufferedWriteBytes, qint64(1));
    }
    writer.releaseWriteBytes(1);
}

void tst_QAMQPFlow::writeQueueDiscardOnDisconnect()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));

    QAmqpClient client;
    QAmqpClientPrivate writer(&client);
    writer.socket = new QSslSocket(&client);
    writer.maxWriteQueueBytes = 10 * 1024 * 1024;
    QObject::connect(writer.socket, &QIODevice::bytesWritten, &client,
                     [&writer](qint64 bytes) { writer._q_bytesWritten(bytes); });
    QObject::connect(writer.socket, &QAbstractSocket::disconnected, &client,
                     [&writer] { writer._q_socketDisconnected(); });
    QSignalSpy connected(writer.socket, &QAbstractSocket::connected);
    QSignalSpy discarded(&client, &QAmqpClient::writeQueueDiscarded);
    QSignalSpy disconnected(&client, &QAmqpClient::disconnected);

    writer.socket->connectToHost(QHostAddress::LocalHost, server.serverPort());
    QVERIFY(connected.wait(1000));
    QTRY_VERIFY(server.hasPendingConnections());
    QTcpSocket *peer = server.nextPendingConnection();
    QVERIFY(peer);

    QVERIFY(writer.sendFrameBatch(QByteArray(8 * 1024 * 1024, 'x')));
    QVERIFY(writer.bufferedWriteBytes > 0);
    peer->abort();
    QVERIFY(disconnected.wait(1000));
    QCOMPARE(discarded.count(), 1);
    QVERIFY(discarded.at(0).at(0).toLongLong() > 0);
    QCOMPARE(writer.bufferedWriteBytes, qint64(0));
}

void tst_QAMQPFlow::flowStateResetOnReconnect()
{
    TestChannel channel;
    channel.receiveMethod(flowFrame(channel.channelNumber(), false));
    QVERIFY(!channel.isFlowActive());

    // a reconnected channel starts out active again, otherwise publishes would
    // stay queued forever waiting for a channel.flow that never arrives
    channel.resetState();
    QVERIFY(channel.isFlowActive());
}

QTEST_APPLESS_MAIN(tst_QAMQPFlow)
#include "tst_qamqpflow.moc"