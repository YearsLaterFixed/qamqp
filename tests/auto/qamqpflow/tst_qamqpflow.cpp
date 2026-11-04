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
    void deletingExchangeReleasesOnlyItsReservations();
    void cancellingDeferredPublishesIsScoped();
    void cancellingDeferredPublishesPreservesWriterBacklog();
    void queuedPublishUsesNegotiatedFrameSize();
    void conservativePublishReservations();
    void publishMetadataFrameLimits();
    void incomingFrameLimitIncludesOverhead();
    void deferredFramingFailureIsObservable();
    void immediateWriterFailureDoesNotRetainReservation();
    void silentPeerExpiresNegotiatedHeartbeat();
    void inboundTrafficKeepsHeartbeatAlive_data();
    void inboundTrafficKeepsHeartbeatAlive();
    void zeroHeartbeatDisablesWatchdog();
    void disconnectStopsHeartbeatTimers();
    void heartbeatTimeoutReconnectsOnce();
    void clientDestructionWithDeferredPublishes();
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

void tst_QAMQPFlow::deletingExchangeReleasesOnlyItsReservations()
{
    QAmqpClient client;
    QAmqpExchange *retained = client.createExchange("retained");
    QCOMPARE(retained->publish("retained payload", "route"), QAmqpExchange::Queued);
    const qlonglong retainedBytes = client.bufferedWriteBytes();
    QVERIFY(retainedBytes > 0);

    for (int iteration = 0; iteration < 32; ++iteration) {
        QAmqpExchange *exchange = client.createExchange("disposable");
        QVERIFY(exchange);
        QCOMPARE(exchange->publish("queued payload", "route"), QAmqpExchange::Queued);
        QVERIFY(client.bufferedWriteBytes() > retainedBytes);
        client.setMaxWriteQueueBytes(client.bufferedWriteBytes());
        delete exchange;
        QCOMPARE(client.bufferedWriteBytes(), retainedBytes);
    }
    delete retained;
    QCOMPARE(client.bufferedWriteBytes(), qlonglong(0));
}

void tst_QAMQPFlow::cancellingDeferredPublishesIsScoped()
{
    QAmqpClient client;
    QAmqpExchange *retained = client.createExchange("retained");
    QCOMPARE(retained->publish("retained payload", "route"), QAmqpExchange::Queued);
    const qlonglong retainedBytes = client.bufferedWriteBytes();
    QAmqpExchange *exchange = client.createExchange("cancelled");
    QSignalSpy cancelled(exchange, &QAmqpExchange::pendingPublishesCancelled);

    for (int iteration = 0; iteration < 32; ++iteration) {
        QCOMPARE(exchange->publish("first payload", "route"), QAmqpExchange::Queued);
        QCOMPARE(exchange->publish("second payload", "route"), QAmqpExchange::Queued);
        const qlonglong cancelledBytes = client.bufferedWriteBytes() - retainedBytes;
        QVERIFY(cancelledBytes > 0);
        client.setMaxWriteQueueBytes(client.bufferedWriteBytes());
        QCOMPARE(exchange->cancelPendingPublishes(), 2);
        QCOMPARE(cancelled.count(), iteration + 1);
        QCOMPARE(cancelled.last().at(0).toInt(), 2);
        QCOMPARE(cancelled.last().at(1).toLongLong(), cancelledBytes);
        QCOMPARE(client.bufferedWriteBytes(), retainedBytes);
        QCOMPARE(exchange->cancelPendingPublishes(), 0);
        QCOMPARE(cancelled.count(), iteration + 1);
        QCOMPARE(client.bufferedWriteBytes(), retainedBytes);
    }
    delete exchange;
    QCOMPARE(client.bufferedWriteBytes(), retainedBytes);
    delete retained;
    QCOMPARE(client.bufferedWriteBytes(), qlonglong(0));
}

void tst_QAMQPFlow::clientDestructionWithDeferredPublishes()
{
    QAmqpClient *client = new QAmqpClient;
    QPointer<QAmqpExchange> exchange = client->createExchange("queued-at-destruction");
    QCOMPARE(exchange->publish("queued payload", "route"), QAmqpExchange::Queued);
    QVERIFY(client->bufferedWriteBytes() > 0);
    delete client;
    QVERIFY(exchange.isNull());
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
        if (maximumWriteSize < 0)
            return -1;
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

void tst_QAMQPFlow::cancellingDeferredPublishesPreservesWriterBacklog()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QAmqpExchange *exchange = client.createExchange("mixed-backlog");
    QAmqpExchange *retained = client.createExchange("retained-backlog");
    QCOMPARE(retained->publish("retained payload", "route"), QAmqpExchange::Queued);
    const qlonglong retainedBytes = client.bufferedWriteBytes();
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);
    PartialWriteSocket *socket = static_cast<PartialWriteSocket *>(writer.socket);
    writer.connected = true;
    QAmqpExchangePrivate *exchangeState = dynamic_cast<QAmqpExchangePrivate *>(
        writer.methodHandlersByChannel.value(exchange->channelNumber()).first());
    QVERIFY(exchangeState);
    exchangeState->opened = true;
    QCOMPARE(exchange->publish(QByteArray(128 * 1024, 'p'), "route", "application/octet-stream"),
             QAmqpExchange::AcceptedForWrite);
    QCOMPARE(writer.pendingWrites.size(), 1);
    QVERIFY(!writer.inFlightWrites.isEmpty());
    QVERIFY(writer.socket->bytesToWrite() > 0);
    const QByteArray expected = writer.pendingWrites.head().data;
    const qlonglong admittedBytes = writer.socketPendingBytes;
    QVERIFY(admittedBytes > 0);

    exchangeState->opened = false;
    QCOMPARE(exchange->publish("cancelled payload", "route"), QAmqpExchange::Queued);
    const qlonglong totalReservedBytes = client.bufferedWriteBytes();
    QVERIFY(totalReservedBytes > retainedBytes + admittedBytes);
    exchangeState->resetInternalState();
    exchangeState->resetInternalState();
    QCOMPARE(client.bufferedWriteBytes(), totalReservedBytes);
    QSignalSpy cancelled(exchange, &QAmqpExchange::pendingPublishesCancelled);
    QCOMPARE(exchange->cancelPendingPublishes(), 1);
    QCOMPARE(cancelled.count(), 1);
    QCOMPARE(cancelled.first().at(1).toLongLong(), totalReservedBytes - retainedBytes - admittedBytes);
    QCOMPARE(exchangeState->pendingPublishBytes, qint64(0));
    QVERIFY(exchangeState->pendingPublishes.isEmpty());
    QCOMPARE(writer.socketPendingBytes, admittedBytes);
    QCOMPARE(client.bufferedWriteBytes(), retainedBytes + admittedBytes);
    QCOMPARE(writer.pendingWrites.size(), 1);
    QCOMPARE(exchange->cancelPendingPublishes(), 0);
    delete exchange;
    QCOMPARE(client.bufferedWriteBytes(), retainedBytes + admittedBytes);

    socket->maximumWriteSize = 4093;
    writer.flushPendingWrites();
    QTRY_COMPARE(peer->bytesAvailable(), qint64(expected.size()));
    QCOMPARE(peer->readAll(), expected);
    QTRY_COMPARE(writer.socketPendingBytes, qint64(0));
    QTRY_COMPARE(client.bufferedWriteBytes(), retainedBytes);
    delete retained;
    QCOMPARE(client.bufferedWriteBytes(), qlonglong(0));
}

void tst_QAMQPFlow::queuedPublishUsesNegotiatedFrameSize()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QAmqpExchange *exchange = client.createExchange("reframed-publish");
    QAmqpExchangePrivate *exchangeState = dynamic_cast<QAmqpExchangePrivate *>(
        writer.methodHandlersByChannel.value(exchange->channelNumber()).first());
    QVERIFY(exchangeState);
    QSignalSpy failures(exchange, &QAmqpExchange::publishFailed);
    QSignalSpy rejected(exchange, &QAmqpExchange::publishRejected);
    for (int attempt = 0; attempt < 3; ++attempt) {
        writer.frameMax = 65536;
        const QByteArray firstPayload(20000, char('a' + attempt));
        const QByteArray secondPayload(7000, char('x' + attempt));
        QCOMPARE(exchange->publish(firstPayload, "route", "application/octet-stream"), QAmqpExchange::Queued);
        QCOMPARE(exchange->publish(secondPayload, "route", "application/octet-stream"), QAmqpExchange::Queued);
        const qlonglong reservedBytes = client.bufferedWriteBytes();
        client.setMaxWriteQueueBytes(reservedBytes);
        QTcpSocket *peer = connectControlWriter(client, server);
        QVERIFY(peer);
        if (attempt == 1) {
            peer->abort();
            QTRY_COMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
            QCOMPARE(client.bufferedWriteBytes(), reservedBytes);
            QCOMPARE(exchangeState->pendingPublishes.size(), 2);
            peer = connectControlWriter(client, server);
            QVERIFY(peer);
        }
        writer.connected = true;
        writer.frameMax = attempt == 2 ? 8192 : 4096;
        const qint64 expectedBytes =
            exchangeState->serializePublish(exchangeState->pendingPublishes.at(0), writer.frameMax).size() +
            exchangeState->serializePublish(exchangeState->pendingPublishes.at(1), writer.frameMax).size();
        exchangeState->opened = true;
        exchangeState->flushPendingPublishes();
        QCOMPARE(exchangeState->pendingPublishBytes, qint64(0));
        QVERIFY(exchangeState->pendingPublishes.isEmpty());
        QTRY_COMPARE(peer->bytesAvailable(), expectedBytes);
        QTRY_COMPARE(writer.bufferedWriteBytes, qint64(0));
        QDataStream input(peer->readAll());
        QByteArray received;
        int methods = 0;
        int headers = 0;
        while (!input.atEnd()) {
            quint8 type = 0;
            quint16 channel = 0;
            quint32 payloadSize = 0;
            input >> type >> channel >> payloadSize;
            QVERIFY(payloadSize + 8 <= quint32(writer.frameMax));
            QCOMPARE(channel, quint16(exchange->channelNumber()));
            QByteArray framePayload(int(payloadSize), char(0));
            QCOMPARE(input.readRawData(framePayload.data(), framePayload.size()), framePayload.size());
            quint8 end = 0;
            input >> end;
            QCOMPARE(end, quint8(QAmqpFrame::FRAME_END));
            if (type == QAmqpFrame::Body)
                received += framePayload;
            else if (type == QAmqpFrame::Method)
                ++methods;
            else if (type == QAmqpFrame::Header)
                ++headers;
        }
        QCOMPARE(methods, 2);
        QCOMPARE(headers, 2);
        QCOMPARE(received, firstPayload + secondPayload);
        peer->abort();
        QTRY_COMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
    }
    QCOMPARE(failures.count(), 0);
    QCOMPARE(rejected.count(), 0);
}

void tst_QAMQPFlow::conservativePublishReservations()
{
    QCOMPARE(QAmqpExchangePrivate::publishReservation(100, 0, 4096), qint64(200));
    QCOMPARE(QAmqpExchangePrivate::publishReservation(100, 4088, 4096), qint64(2 * (100 + 4088 + 8)));
    QCOMPARE(QAmqpExchangePrivate::publishReservation(100, 4089, 4096), qint64(2 * (100 + 4089 + 16)));
    QAmqpClient client;
    QAmqpExchange *exchange = client.createExchange("reservation-boundary");
    QCOMPARE(exchange->publish(QByteArray(4089, 'p'), "route", "text/plain"), QAmqpExchange::Queued);
    const qlonglong required = client.bufferedWriteBytes();
    QCOMPARE(exchange->cancelPendingPublishes(), 1);
    client.setMaxWriteQueueBytes(required - 1);
    QCOMPARE(exchange->publish(QByteArray(4089, 'p'), "route", "text/plain"), QAmqpExchange::RejectedLimit);
    QCOMPARE(client.bufferedWriteBytes(), qlonglong(0));
    client.setMaxWriteQueueBytes(required);
    QCOMPARE(exchange->publish(QByteArray(4089, 'p'), "route", "text/plain"), QAmqpExchange::Queued);
    QCOMPARE(client.bufferedWriteBytes(), required);
}

void tst_QAMQPFlow::publishMetadataFrameLimits()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QAmqpMethodFrame method(QAmqpFrame::Basic, 40);
    method.setChannel(1);
    method.setArguments(QByteArray(4084, 'm'));
    QCOMPARE(encodeFrame(method).size(), 4096);
    QVERIFY(QAmqpExchangePrivate::headerFitsFrameLimit(encodeFrame(method), 4096));
    method.setArguments(QByteArray(4085, 'm'));
    QVERIFY(!QAmqpExchangePrivate::headerFitsFrameLimit(encodeFrame(method), 4096));

    QAmqpTable headers;
    headers.insert("large", QByteArray());
    QAmqpContentFrame content(QAmqpFrame::Basic);
    content.setChannel(1);
    content.setBodySize(0);
    content.setProperty(QAmqpMessage::Headers, headers);
    const int baseBytes = encodeFrame(content).size();
    headers.insert("large", QByteArray(4096 - baseBytes, 'h'));
    content.setProperty(QAmqpMessage::Headers, headers);
    QCOMPARE(encodeFrame(content).size(), 4096);
    QVERIFY(QAmqpExchangePrivate::headerFitsFrameLimit(encodeFrame(content), 4096));
    headers.insert("large", QByteArray(4097 - baseBytes, 'h'));
    content.setProperty(QAmqpMessage::Headers, headers);
    QVERIFY(!QAmqpExchangePrivate::headerFitsFrameLimit(encodeFrame(content), 4096));

    ControlTestClient client;
    QAmqpExchange *exchange = client.createExchange("metadata-limit");
    QSignalSpy rejected(exchange, &QAmqpExchange::publishRejected);
    QCOMPARE(exchange->publish(QByteArray("payload"), "route", "text/plain", headers),
             QAmqpExchange::RejectedLimit);
    QCOMPARE(rejected.count(), 1);
    QCOMPARE(rejected.first().at(0).value<QAmqpExchange::PublishError>(),
             QAmqpExchange::FrameSizeLimitExceeded);
    QCOMPARE(client.bufferedWriteBytes(), qlonglong(0));

    QTcpServer server;
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);
    QAmqpClientPrivate &writer = *client.state();
    writer.connected = true;
    writer.frameMax = 8192;
    QAmqpExchangePrivate *exchangeState = dynamic_cast<QAmqpExchangePrivate *>(
        writer.methodHandlersByChannel.value(exchange->channelNumber()).first());
    QVERIFY(exchangeState);
    exchangeState->opened = true;
    QCOMPARE(exchange->publish(QByteArray("payload"), "route", "text/plain", headers),
             QAmqpExchange::AcceptedForWrite);
    const qint64 expectedBytes = writer.socketPendingBytes;
    QTRY_COMPARE(peer->bytesAvailable(), expectedBytes);
    QVERIFY(QAmqpExchangePrivate::headerFitsFrameLimit(peer->readAll(), 8192));
    QTRY_COMPARE(client.bufferedWriteBytes(), qlonglong(0));
    QCOMPARE(rejected.count(), 1);
    exchangeState->flowActive = false;
    QCOMPARE(exchange->publish(QByteArray("payload"), "route", "text/plain", headers),
             QAmqpExchange::RejectedLimit);
    QCOMPARE(rejected.count(), 2);
    QCOMPARE(client.bufferedWriteBytes(), qlonglong(0));
}

void tst_QAMQPFlow::incomingFrameLimitIncludesOverhead()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);
    writer.frameMax = 4096;
    QByteArray header;
    QDataStream headerStream(&header, QIODevice::WriteOnly);
    headerStream << quint8(QAmqpFrame::Body) << quint16(1) << quint32(4089);
    peer->write(header);
    peer->flush();
    QTRY_COMPARE(writer.socket->bytesAvailable(), qint64(header.size()));
    writer._q_readyRead();
    QTRY_VERIFY(peer->bytesAvailable() > 0);
    QDataStream response(peer->readAll());
    QAmqpMethodFrame close;
    response >> close;
    QCOMPARE(close.methodClass(), QAmqpFrame::Connection);
    QCOMPARE(close.id(), qint16(QAmqpClientPrivate::miClose));
    QDataStream arguments(close.arguments());
    quint16 code = 0;
    arguments >> code;
    QCOMPARE(code, quint16(QAMQP::FrameError));
    writer.socket->readAll();
    writer.frameMax = 0;
    QSignalSpy heartbeats(&client, &QAmqpClient::heartbeat);
    QAmqpHeartbeatFrame heartbeat;
    peer->write(encodeFrame(heartbeat));
    peer->flush();
    QTRY_COMPARE(writer.socket->bytesAvailable(), qint64(8));
    writer._q_readyRead();
    QCOMPARE(heartbeats.count(), 1);
}

void tst_QAMQPFlow::deferredFramingFailureIsObservable()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QAmqpExchange *exchange = client.createExchange("failed-framing");
    QAmqpExchange *retained = client.createExchange("retained-framing");
    QCOMPARE(retained->publish("retained", "route"), QAmqpExchange::Queued);
    const qlonglong retainedBytes = client.bufferedWriteBytes();
    QCOMPARE(exchange->publish("queued", "route"), QAmqpExchange::Queued);
    QSignalSpy failures(exchange, &QAmqpExchange::publishFailed);
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);
    writer.connected = true;
    writer.frameMax = 2048;
    QAmqpExchangePrivate *exchangeState = dynamic_cast<QAmqpExchangePrivate *>(
        writer.methodHandlersByChannel.value(exchange->channelNumber()).first());
    QVERIFY(exchangeState);
    exchangeState->opened = true;
    exchangeState->flushPendingPublishes();
    QCOMPARE(failures.count(), 1);
    QCOMPARE(failures.first().at(0).value<QAmqpExchange::PublishError>(),
             QAmqpExchange::FrameSizeLimitExceeded);
    QVERIFY(exchangeState->pendingPublishes.isEmpty());
    QCOMPARE(client.bufferedWriteBytes(), retainedBytes);
    QCOMPARE(peer->bytesAvailable(), qint64(0));
}

void tst_QAMQPFlow::immediateWriterFailureDoesNotRetainReservation()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QAmqpExchange *exchange = client.createExchange("failed-writer");
    QCOMPARE(exchange->publish("queued payload", "route"), QAmqpExchange::Queued);
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);
    static_cast<PartialWriteSocket *>(writer.socket)->maximumWriteSize = -1;
    writer.connected = true;
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    QSignalSpy discarded(&client, &QAmqpClient::writeQueueDiscarded);
    QAmqpExchangePrivate *exchangeState = dynamic_cast<QAmqpExchangePrivate *>(
        writer.methodHandlersByChannel.value(exchange->channelNumber()).first());
    QVERIFY(exchangeState);
    exchangeState->opened = true;
    exchangeState->flushPendingPublishes();
    QCOMPARE(errors.count(), 1);
    QCOMPARE(discarded.count(), 1);
    QVERIFY(discarded.first().at(0).toLongLong() > 0);
    QCOMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
    QVERIFY(exchangeState->pendingPublishes.isEmpty());
    QCOMPARE(exchangeState->pendingPublishBytes, qint64(0));
    QCOMPARE(client.bufferedWriteBytes(), qlonglong(0));
    QCOMPARE(exchange->cancelPendingPublishes(), 0);
}

void tst_QAMQPFlow::silentPeerExpiresNegotiatedHeartbeat()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QVERIFY(connectControlWriter(client, server));
    writer.connected = true;
    client.setAutoReconnect(true, 1000);
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    QSignalSpy disconnected(&client, &QAmqpClient::disconnected);
    QAmqpMethodFrame tune(QAmqpFrame::Connection, QAmqpClientPrivate::miTune);
    QByteArray arguments;
    QDataStream stream(&arguments, QIODevice::WriteOnly);
    stream << quint16(16) << quint32(4096) << quint16(1);
    tune.setArguments(arguments);
    writer.tune(tune);
    QTRY_COMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
    QCOMPARE(errors.count(), 1);
    QCOMPARE(disconnected.count(), 1);
    QVERIFY(client.errorString().contains("heartbeat", Qt::CaseInsensitive));
    QVERIFY(writer.reconnectTimer->isActive());
    writer.reconnectTimer->stop();
}

static QAmqpMethodFrame heartbeatTune(quint16 seconds)
{
    QAmqpMethodFrame tune(QAmqpFrame::Connection, QAmqpClientPrivate::miTune);
    QByteArray arguments;
    QDataStream stream(&arguments, QIODevice::WriteOnly);
    stream << quint16(16) << quint32(4096) << seconds;
    tune.setArguments(arguments);
    return tune;
}

class HeartbeatTrafficProbe : public QAmqpMethodFrameHandler
{
public:
    HeartbeatTrafficProbe() : calls(0) {}
    bool _q_method(const QAmqpMethodFrame &) override { ++calls; return true; }
    int calls;
};

void tst_QAMQPFlow::inboundTrafficKeepsHeartbeatAlive_data()
{
    QTest::addColumn<bool>("heartbeatFrames");
    QTest::newRow("ordinary-method-traffic") << false;
    QTest::newRow("heartbeat-traffic") << true;
}

void tst_QAMQPFlow::inboundTrafficKeepsHeartbeatAlive()
{
    QFETCH(bool, heartbeatFrames);
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);
    writer.connected = true;
    HeartbeatTrafficProbe probe;
    writer.methodHandlersByChannel[1].append(&probe);
    QObject::connect(writer.socket, &QIODevice::readyRead, &client, [&writer] { writer._q_readyRead(); });
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    QSignalSpy heartbeats(&client, &QAmqpClient::heartbeat);
    writer.tune(heartbeatTune(1));
    QCOMPARE(writer.heartbeatTimer->interval(), 500);
    QAmqpMethodFrame method(QAmqpFrame::Basic, QAmqpChannelPrivate::bmQosOk);
    method.setChannel(1);
    QAmqpHeartbeatFrame heartbeat;
    const QByteArray activity = heartbeatFrames ? encodeFrame(heartbeat) : encodeFrame(method);
    QTimer traffic;
    QObject::connect(&traffic, &QTimer::timeout, &client, [peer, activity] {
        peer->write(activity);
        peer->flush();
    });
    traffic.start(100);
    bool observedActivePeriod = false;
    QTimer::singleShot(1500, &client, [&observedActivePeriod] { observedActivePeriod = true; });
    QTRY_VERIFY(observedActivePeriod);
    QCOMPARE(errors.count(), 0);
    QCOMPARE(writer.socket->state(), QAbstractSocket::ConnectedState);
    QVERIFY(heartbeatFrames ? heartbeats.count() > 0 : probe.calls > 0);
    traffic.stop();
    QTRY_COMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
    QCOMPARE(errors.count(), 1);
    QVERIFY(!writer.heartbeatTimer->isActive());
    QVERIFY(!writer.receiveHeartbeatTimer->isActive());
    QVERIFY(!writer.lastReceived.isValid());
    QVERIFY(!writer.reconnectTimer->isActive());
    for (int attempt = 0; attempt < 4; ++attempt)
        writer._q_checkHeartbeat();
    QCOMPARE(errors.count(), 1);
}

void tst_QAMQPFlow::zeroHeartbeatDisablesWatchdog()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QVERIFY(connectControlWriter(client, server));
    writer.connected = true;
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    writer.tune(heartbeatTune(1));
    QVERIFY(writer.receiveHeartbeatTimer->isActive());
    writer.heartbeatDelay = 0;
    writer.tune(heartbeatTune(0));
    QVERIFY(!writer.heartbeatTimer->isActive());
    QVERIFY(!writer.receiveHeartbeatTimer->isActive());
    QVERIFY(!writer.lastReceived.isValid());
    bool observedSilentPeriod = false;
    QTimer::singleShot(1200, &client, [&observedSilentPeriod] { observedSilentPeriod = true; });
    QTRY_VERIFY(observedSilentPeriod);
    writer._q_checkHeartbeat();
    QCOMPARE(errors.count(), 0);
    QCOMPARE(writer.socket->state(), QAbstractSocket::ConnectedState);
}

void tst_QAMQPFlow::disconnectStopsHeartbeatTimers()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    ControlTestClient client;
    QAmqpClientPrivate &writer = *client.state();
    QTcpSocket *peer = connectControlWriter(client, server);
    QVERIFY(peer);
    writer.connected = true;
    writer.tune(heartbeatTune(1));
    peer->abort();
    QTRY_COMPARE(writer.socket->state(), QAbstractSocket::UnconnectedState);
    QVERIFY(!writer.heartbeatTimer->isActive());
    QVERIFY(!writer.receiveHeartbeatTimer->isActive());
    QVERIFY(!writer.lastReceived.isValid());
    peer = connectControlWriter(client, server);
    QVERIFY(peer);
    writer.connected = true;
    writer.tune(heartbeatTune(1));
    writer.closed = true;
    writer._q_checkHeartbeat();
    QVERIFY(!writer.heartbeatTimer->isActive());
    QVERIFY(!writer.receiveHeartbeatTimer->isActive());
    QVERIFY(!writer.lastReceived.isValid());
    writer.closed = false;
    writer.tune(heartbeatTune(1));
    writer._q_disconnect();
    QVERIFY(!writer.heartbeatTimer->isActive());
    QVERIFY(!writer.receiveHeartbeatTimer->isActive());
    QVERIFY(!writer.lastReceived.isValid());
    writer.socket->abort();
}

void tst_QAMQPFlow::heartbeatTimeoutReconnectsOnce()
{
    int argc = 0;
    char *argv[] = {nullptr};
    QCoreApplication app(argc, argv);
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    ControlTestClient client;
    client.setHost("127.0.0.1");
    client.setPort(server.serverPort());
    client.setAutoReconnect(true, 50);
    QAmqpClientPrivate &writer = *client.state();
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy disconnected(&client, &QAmqpClient::disconnected);
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    int connections = 0;
    QPointer<QTcpSocket> responsivePeer;
    QTimer traffic;
    QObject::connect(&traffic, &QTimer::timeout, &client, [&responsivePeer] {
        if (responsivePeer) {
            QAmqpHeartbeatFrame heartbeat;
            responsivePeer->write(encodeFrame(heartbeat));
            responsivePeer->flush();
        }
    });
    QObject::connect(&server, &QTcpServer::newConnection, &client, [&] {
        QTcpSocket *peer = server.nextPendingConnection();
        ++connections;
        QAmqpMethodFrame openOk(QAmqpFrame::Connection, QAmqpClientPrivate::miOpenOk);
        openOk.setArguments(QByteArray(1, char(0)));
        peer->write(encodeFrame(heartbeatTune(1)) + encodeFrame(openOk));
        peer->flush();
        if (connections > 1) {
            responsivePeer = peer;
            traffic.start(100);
        }
    });
    client.connectToHost();
    QTRY_COMPARE(connected.count(), 2);
    QCOMPARE(connections, 2);
    QCOMPARE(disconnected.count(), 1);
    QCOMPARE(errors.count(), 1);
    QVERIFY(client.isConnected());
    QVERIFY(!writer.reconnectTimer->isActive());
    QVERIFY(writer.receiveHeartbeatTimer->isActive());
    bool observedRecoveredPeriod = false;
    QTimer::singleShot(1500, &client, [&observedRecoveredPeriod] { observedRecoveredPeriod = true; });
    QTRY_VERIFY(observedRecoveredPeriod);
    QCOMPARE(connections, 2);
    QCOMPARE(errors.count(), 1);
    QCOMPARE(connected.count(), 2);
    client.setAutoReconnect(false);
    traffic.stop();
    client.abort();
    QTRY_COMPARE(disconnected.count(), 2);
    QVERIFY(!writer.receiveHeartbeatTimer->isActive());
    QVERIFY(!writer.lastReceived.isValid());
    QVERIFY(!writer.reconnectTimer->isActive());
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