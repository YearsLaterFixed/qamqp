#include <QtTest/QtTest>
/* Fork modifications Copyright (C) 2026 Fabian Suhr (YearsLaterFixed).
 * Modified: 2026-11-04. See LICENSING.md at repository root for inherited rights.
 * Fork/Contact: https://github.com/YearsLaterFixed/qamqp */
#include <QProcess>
#include <QSslSocket>
#include <QTcpServer>
#include <QTcpSocket>
#include <QTimer>
#include <QUuid>

#include "qamqptestcase.h"
#include "qamqpauthenticator.h"
#include "qamqpexchange.h"
#include "qamqpqueue.h"
#include "qamqpclient_p.h"
#include "qamqpclient.h"

class tst_QAMQPClient : public TestCase
{
    Q_OBJECT
private Q_SLOTS:
    void connect();
    void connectProperties();
    void connectHostAddress();
    void connectDisconnect();
    void invalidAuthenticationMechanism();
    void tune();
    void channelMaxNegotiation_data();
    void channelMaxNegotiation();
    void frameMaxNegotiation_data();
    void frameMaxNegotiation();
    void heartbeatNegotiation_data();
    void heartbeatNegotiation();
    void protocolMaximums();
    void clientPropertiesMerge();
    void writeQueueReservation_data();
    void writeQueueReservation();
    void channelAllocationLimit();
    void socketError();
    void invalidUri_data();
    void invalidUri();
    void validateUri_data();
    void validateUri();
    void issue38();
    void issue38_take2();

    void autoReconnect();
    void autoReconnectTimeout();
    void sslConnect();
    void sslRejectsUntrustedCertificate();
    void sslRejectsWrongPeerName();
    void multiChannelReconnectSoak();

private:
    QSslConfiguration createSslConfiguration();
    void issue38_helper(QAmqpClient *client);

};

class ReconnectTestClient : public QAmqpClient
{
public:
    QAmqpClientPrivate *state() { return d_func(); }
};

static bool runBrokerCommand(const QString &command)
{
    QProcess process;
    process.start("rabbitmqctl", QStringList() << command);
    if (!process.waitForStarted(5000))
        return false;
    if (!process.waitForFinished(30000)) {
        process.kill();
        process.waitForFinished(5000);
        return false;
    }
    return process.exitStatus() == QProcess::NormalExit && process.exitCode() == 0;
}

class BrokerRestartGuard
{
public:
    BrokerRestartGuard() : stopped(false) {}
    ~BrokerRestartGuard() { if (stopped) runBrokerCommand("start_app"); }
    bool stopped;
};

void tst_QAMQPClient::multiChannelReconnectSoak()
{
    bool configured = false;
    const int requested = qEnvironmentVariableIntValue("QAMQP_SOAK_CYCLES", &configured);
    QVERIFY2(qEnvironmentVariableIsEmpty("QAMQP_SOAK_CYCLES") || configured,
             "QAMQP_SOAK_CYCLES must be an integer");
    const int cycles = configured ? requested : 3;
    QVERIFY(cycles >= 1 && cycles <= 10000);
    const int streams = 6;
    const int perStream = 3;
    QVector<int> consuming(streams, 0), received(streams, 0);
    QList<QAmqpQueue *> queues;
    QList<QAmqpExchange *> exchanges;
    ReconnectTestClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    const QString prefix = "soak-" + QUuid::createUuid().toString(QUuid::WithoutBraces);
    for (int index = 0; index < streams; ++index) {
        const QString name = prefix + QString::number(index);
        QAmqpExchange *exchange = client.createExchange(name);
        QAmqpQueue *queue = client.createQueue(name);
        QVERIFY(exchange);
        QVERIFY(queue);
        queue->setMaxBufferedMessages(perStream);
        queue->setMaxBufferedBytes(perStream * 16384);
        queue->setMaxMessageSize(16384);
        exchanges.append(exchange);
        queues.append(queue);
        QObject::connect(&client, &QAmqpClient::connected, &client,
                         [exchange] { exchange->declare(QAmqpExchange::FanOut, QAmqpExchange::Durable); });
        QObject::connect(exchange, &QAmqpExchange::declared, &client,
                         [queue] { queue->declare(QAmqpQueue::Durable); });
        QObject::connect(queue, &QAmqpQueue::declared, &client,
                         [queue, exchange] { queue->bind(exchange, "route"); });
        QObject::connect(queue, &QAmqpQueue::bound, &client,
                         [queue] { queue->consume(QAmqpQueue::coNoAck); });
        QObject::connect(queue, &QAmqpQueue::consuming, &client,
                         [&, index](const QString &) { ++consuming[index]; });
        QObject::connect(queue, &QAmqpQueue::messageReceived, &client,
                         [&, index] { ++received[index]; });
    }
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy disconnected(&client, &QAmqpClient::disconnected);
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    QElapsedTimer elapsed;
    elapsed.start();
    for (int cycle = 0; cycle < cycles; ++cycle) {
        client.connectToHost();
        QTRY_COMPARE(connected.count(), cycle + 1);
        for (int index = 0; index < streams; ++index) {
            QTRY_COMPARE(consuming[index], cycle + 1);
            QCOMPARE(client.createExchange(exchanges[index]->name()), exchanges[index]);
            QCOMPARE(client.createQueue(queues[index]->name()), queues[index]);
            for (int message = 0; message < perStream; ++message) {
                const QByteArray payload = QByteArray::number(cycle) + ':' +
                    QByteArray::number(index) + ':' + QByteArray::number(message) +
                    QByteArray(8192, char('a' + index));
                QCOMPARE(exchanges[index]->publish(payload, "route", "application/octet-stream"),
                         QAmqpExchange::AcceptedForWrite);
            }
        }
        for (int index = 0; index < streams; ++index) {
            QTRY_COMPARE(received[index], (cycle + 1) * perStream);
            QCOMPARE(queues[index]->bufferedMessageCount(), perStream);
            for (int message = 0; message < perStream; ++message) {
                const QByteArray expected = QByteArray::number(cycle) + ':' +
                    QByteArray::number(index) + ':' + QByteArray::number(message) +
                    QByteArray(8192, char('a' + index));
                QCOMPARE(queues[index]->dequeue().payload(), expected);
            }
            QCOMPARE(queues[index]->bufferedMessageBytes(), qlonglong(0));
            QCOMPARE(queues[index]->bufferedMessageCount(), 0);
        }
        QTRY_COMPARE(client.bufferedWriteBytes(), qlonglong(0));
        QCOMPARE(client.findChildren<QAmqpQueue *>().size(), streams);
        QCOMPARE(client.findChildren<QAmqpExchange *>().size(), streams);
        QCOMPARE(client.state()->nextChannelNumber, quint16(streams * 2));
        client.disconnectFromHost();
        QTRY_COMPARE(disconnected.count(), cycle + 1);
        QCOMPARE(client.state()->bufferedControlBytes, qint64(0));
        QCOMPARE(client.state()->socketPendingBytes, qint64(0));
        QVERIFY(client.state()->pendingWrites.isEmpty());
        QVERIFY(client.state()->controlWrites.isEmpty());
        QVERIFY(client.state()->inFlightWrites.isEmpty());
        QVERIFY(client.state()->pendingChannelCloses.isEmpty());
        QCOMPARE(errors.count(), 0);
    }
    qInfo() << "Reconnect soak PASS: cycles" << cycles << "channels" << streams * 2
            << "messages" << cycles * streams * perStream << "elapsed-ms" << elapsed.elapsed()
            << "final-buffered-bytes" << client.bufferedWriteBytes();
}

QSslConfiguration tst_QAMQPClient::createSslConfiguration()
{
    QSslConfiguration sslConfiguration = QSslConfiguration::defaultConfiguration();
    sslConfiguration.setCaCertificates(QSslCertificate::fromPath(
        qEnvironmentVariable("QAMQP_TEST_CA_CERT")));
    sslConfiguration.setPeerVerifyMode(QSslSocket::VerifyPeer);
    sslConfiguration.setProtocol(QSsl::TlsV1_2OrLater);
    return sslConfiguration;
}

void tst_QAMQPClient::connect()
{
    QAmqpClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    client.connectToHost();
    QVERIFY(waitForSignal(&client, SIGNAL(connected())));

    QCOMPARE(client.host(), QLatin1String(AMQP_HOST));
    QCOMPARE(client.port(), quint16(AMQP_PORT));
    QCOMPARE(client.virtualHost(), QLatin1String(AMQP_VHOST));
    QCOMPARE(client.username(), QLatin1String("guest"));
    QCOMPARE(client.password(), QLatin1String("guest"));
    QCOMPARE(client.autoReconnect(), false);

    client.disconnectFromHost();
    QVERIFY(waitForSignal(&client, SIGNAL(disconnected())));
}

void tst_QAMQPClient::sslConnect()
{
    QAmqpClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    client.setSslConfiguration(createSslConfiguration());
    QVERIFY(QSslSocket::supportsSsl());
    QVERIFY2(!client.sslConfiguration().caCertificates().isEmpty(),
             "Set QAMQP_TEST_CA_CERT to the freshly generated test CA certificate");
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy errors(&client, &QAmqpClient::sslErrors);
    client.connectToHost();
    QTRY_COMPARE(connected.count(), 1);
    QCOMPARE(errors.count(), 0);
    QCOMPARE(client.port(), quint16(5671));
    QVERIFY(!client.sslConfiguration().peerCertificate().isNull());
    QCOMPARE(client.sslConfiguration().peerVerifyMode(), QSslSocket::VerifyPeer);
    client.disconnectFromHost();
    QTRY_VERIFY(!client.isConnected());
}

void tst_QAMQPClient::sslRejectsUntrustedCertificate()
{
    QAmqpClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    QSslConfiguration configuration = createSslConfiguration();
    configuration.setCaCertificates(QList<QSslCertificate>());
    client.setSslConfiguration(configuration);
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy errors(&client, &QAmqpClient::sslErrors);
    client.connectToHost();
    QTRY_VERIFY(!errors.isEmpty());
    QTRY_COMPARE(client.socketState(), QAbstractSocket::UnconnectedState);
    QCOMPARE(connected.count(), 0);
    QVERIFY(!client.isConnected());
}

void tst_QAMQPClient::sslRejectsWrongPeerName()
{
    ReconnectTestClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    client.setSslConfiguration(createSslConfiguration());
    QVERIFY(!client.sslConfiguration().caCertificates().isEmpty());
    client.state()->socket->setPeerVerifyName("not-localhost.invalid");
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy errors(&client, &QAmqpClient::sslErrors);
    client.connectToHost();
    QTRY_VERIFY(!errors.isEmpty());
    QTRY_COMPARE(client.socketState(), QAbstractSocket::UnconnectedState);
    QCOMPARE(connected.count(), 0);
    bool hostnameMismatch = false;
    const QList<QSslError> failures = errors.first().at(0).value<QList<QSslError> >();
    foreach (const QSslError &failure, failures)
        hostnameMismatch = hostnameMismatch || failure.error() == QSslError::HostNameMismatch;
    QVERIFY(hostnameMismatch);
}

void tst_QAMQPClient::connectProperties()
{
    QAmqpClient client;
    client.setHost("localhost");
    client.setPort(5672);
    client.setVirtualHost("/");
    client.setUsername("guest");
    client.setPassword("guest");
    client.setAutoReconnect(false);
    client.connectToHost();
    QVERIFY(waitForSignal(&client, SIGNAL(connected())));
    client.disconnectFromHost();
    QVERIFY(waitForSignal(&client, SIGNAL(disconnected())));
}

void tst_QAMQPClient::connectHostAddress()
{
    QAmqpClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    client.connectToHost(QHostAddress::LocalHost, 5672);
    QVERIFY(waitForSignal(&client, SIGNAL(connected())));
    client.disconnectFromHost();
    QVERIFY(waitForSignal(&client, SIGNAL(disconnected())));
}

void tst_QAMQPClient::connectDisconnect()
{
    QAmqpClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    client.connectToHost();
    QVERIFY(waitForSignal(&client, SIGNAL(connected())));
    client.disconnectFromHost();
    QVERIFY(waitForSignal(&client, SIGNAL(disconnected())));
}

class InvalidAuthenticator : public QAmqpAuthenticator
{
public:
    virtual QString type() const { return "CRAZYAUTH"; }
    virtual void write(QDataStream &out) {
        Q_UNUSED(out);
    }
};

void tst_QAMQPClient::invalidAuthenticationMechanism()
{
    QAmqpClient client;
    client.setAuth(new InvalidAuthenticator);
    client.connectToHost();
    QVERIFY(waitForSignal(&client, SIGNAL(disconnected())));
}

void tst_QAMQPClient::autoReconnect()
{
    ReconnectTestClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    client.setAutoReconnect(true, 100);
    const QString name = "reconnect-" + QUuid::createUuid().toString(QUuid::WithoutBraces);
    QAmqpExchange *exchange = client.createExchange(name);
    QAmqpQueue *queue = client.createQueue(name);
    QVERIFY(exchange);
    QVERIFY(queue);
    const int exchangeChannel = exchange->channelNumber();
    const int queueChannel = queue->channelNumber();
    QObject::connect(&client, &QAmqpClient::connected, &client,
                     [exchange] { exchange->declare(QAmqpExchange::Direct, QAmqpExchange::Durable); });
    QObject::connect(exchange, &QAmqpExchange::declared, &client,
                     [queue] { queue->declare(QAmqpQueue::Durable); });
    QObject::connect(queue, &QAmqpQueue::declared, &client,
                     [queue, exchange] { queue->bind(exchange, "route"); });
    QObject::connect(queue, &QAmqpQueue::bound, &client,
                     [queue] { queue->consume(QAmqpQueue::coNoAck); });
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy disconnected(&client, &QAmqpClient::disconnected);
    QSignalSpy consuming(queue, &QAmqpQueue::consuming);
    QSignalSpy received(queue, &QAmqpQueue::messageReceived);
    client.connectToHost();
    QTRY_COMPARE(consuming.count(), 1);
    QCOMPARE(exchange->publish("before reconnect", "route"), QAmqpExchange::AcceptedForWrite);
    QTRY_COMPARE(received.count(), 1);
    QCOMPARE(queue->dequeue().payload(), QByteArray("before reconnect"));
    BrokerRestartGuard restart;
    restart.stopped = true;
    QVERIFY2(runBrokerCommand("stop_app"), "rabbitmqctl stop_app must succeed");
    QTRY_COMPARE(disconnected.count(), 1);
    QVERIFY2(runBrokerCommand("start_app"), "rabbitmqctl start_app must succeed");
    restart.stopped = false;
    QTRY_COMPARE_WITH_TIMEOUT(consuming.count(), 2, 15000);
    QCOMPARE(connected.count(), 2);
    QCOMPARE(client.createExchange(name), exchange);
    QCOMPARE(client.createQueue(name), queue);
    QCOMPARE(exchange->channelNumber(), exchangeChannel);
    QCOMPARE(queue->channelNumber(), queueChannel);
    QVERIFY(queue->isConsuming());
    QCOMPARE(exchange->publish("after reconnect", "route"), QAmqpExchange::AcceptedForWrite);
    QTRY_COMPARE(received.count(), 2);
    QCOMPARE(queue->dequeue().payload(), QByteArray("after reconnect"));
    QCOMPARE(queue->bufferedMessageCount(), 0);
    QVERIFY(!client.state()->reconnectTimer->isActive());
    client.setAutoReconnect(false);
    QSignalSpy queueRemoved(queue, &QAmqpQueue::removed);
    queue->remove(QAmqpQueue::roForce);
    QTRY_COMPARE(queueRemoved.count(), 1);
    QSignalSpy exchangeRemoved(exchange, &QAmqpExchange::removed);
    exchange->remove(QAmqpExchange::roForce);
    QTRY_COMPARE(exchangeRemoved.count(), 1);
    client.disconnectFromHost();
    QTRY_COMPARE(disconnected.count(), 2);
}

void tst_QAMQPClient::autoReconnectTimeout()
{
    QTcpServer server;
    QVERIFY(server.listen(QHostAddress::LocalHost, 0));
    const quint16 port = server.serverPort();
    server.close();
    ReconnectTestClient client;
    client.setHost("127.0.0.1");
    client.setPort(port);
    client.setUsername("guest");
    client.setPassword("guest");
    client.setAutoReconnect(true, 100);
    QSignalSpy connected(&client, &QAmqpClient::connected);
    QSignalSpy errors(&client, &QAmqpClient::socketErrorOccurred);
    client.connectToHost();
    QTRY_VERIFY(errors.count() >= 3);
    QCOMPARE(client.state()->reconnectTimer->interval(), 100);
    QVERIFY(server.listen(QHostAddress::LocalHost, port));
    int connections = 0;
    QObject::connect(&server, &QTcpServer::newConnection, &client, [&] {
        QTcpSocket *peer = server.nextPendingConnection();
        ++connections;
        QAmqpMethodFrame tune(QAmqpFrame::Connection, QAmqpClientPrivate::miTune);
        QByteArray arguments;
        QDataStream stream(&arguments, QIODevice::WriteOnly);
        stream << quint16(16) << quint32(4096) << quint16(0);
        tune.setArguments(arguments);
        QAmqpMethodFrame open(QAmqpFrame::Connection, QAmqpClientPrivate::miOpenOk);
        open.setArguments(QByteArray(1, char(0)));
        QByteArray wire;
        QDataStream output(&wire, QIODevice::WriteOnly);
        output << tune << open;
        peer->write(wire);
        peer->flush();
    });
    QTRY_COMPARE(connected.count(), 1);
    QVERIFY(client.isConnected());
    QVERIFY(!client.state()->reconnectTimer->isActive());
    bool observedStableConnection = false;
    QTimer::singleShot(400, &client, [&observedStableConnection] { observedStableConnection = true; });
    QTRY_VERIFY(observedStableConnection);
    QCOMPARE(connections, 1);
    QCOMPARE(connected.count(), 1);
    client.setAutoReconnect(false);
    client.abort();
}

void tst_QAMQPClient::tune()
{
    QAmqpClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    client.setChannelMax(15);
    client.setFrameMax(5000);
    client.setHeartbeatDelay(600);

    client.connectToHost();
    QVERIFY(waitForSignal(&client, SIGNAL(connected())));
    QCOMPARE((int) client.channelMax(), 15);
    QCOMPARE((int)client.heartbeatDelay(), 60);
    QCOMPARE((int)client.frameMax(), 5000);

    client.disconnectFromHost();
    QVERIFY(waitForSignal(&client, SIGNAL(disconnected())));
}

void tst_QAMQPClient::channelMaxNegotiation_data()
{
    QTest::addColumn<quint16>("clientChannelMax");
    QTest::addColumn<quint16>("serverChannelMax");
    QTest::addColumn<quint16>("expectedChannelMax");

    QTest::newRow("both-unlimited") << quint16(0) << quint16(0) << quint16(0);
    QTest::newRow("client-unlimited") << quint16(0) << quint16(10) << quint16(10);
    QTest::newRow("server-unlimited") << quint16(10) << quint16(0) << quint16(10);
    QTest::newRow("client-lower") << quint16(10) << quint16(20) << quint16(10);
    QTest::newRow("server-lower") << quint16(20) << quint16(10) << quint16(10);
    QTest::newRow("client-above-signed-range") << quint16(32768) << quint16(65535) << quint16(32768);
    QTest::newRow("server-above-signed-range") << quint16(65535) << quint16(32768) << quint16(32768);
    QTest::newRow("protocol-maximum") << quint16(65535) << quint16(65535) << quint16(65535);
}

void tst_QAMQPClient::channelMaxNegotiation()
{
    QFETCH(quint16, clientChannelMax);
    QFETCH(quint16, serverChannelMax);
    QFETCH(quint16, expectedChannelMax);

    QCOMPARE(QAmqpClientPrivate::negotiateChannelMax(clientChannelMax, serverChannelMax), expectedChannelMax);
}

void tst_QAMQPClient::frameMaxNegotiation_data()
{
    QTest::addColumn<qint32>("clientFrameMax");
    QTest::addColumn<qint32>("serverFrameMax");
    QTest::addColumn<qint32>("expectedFrameMax");

    QTest::newRow("both-unlimited") << qint32(0) << qint32(0) << qint32(0);
    QTest::newRow("client-unlimited") << qint32(0) << qint32(8192) << qint32(8192);
    QTest::newRow("server-unlimited") << qint32(8192) << qint32(0) << qint32(8192);
    QTest::newRow("client-lower") << qint32(8192) << qint32(131072) << qint32(8192);
    QTest::newRow("server-lower") << qint32(131072) << qint32(8192) << qint32(8192);
    QTest::newRow("below-amqp-floor") << qint32(1024) << qint32(2048) << qint32(AMQP_FRAME_MIN_SIZE);
}

void tst_QAMQPClient::frameMaxNegotiation()
{
    QFETCH(qint32, clientFrameMax);
    QFETCH(qint32, serverFrameMax);
    QFETCH(qint32, expectedFrameMax);

    QCOMPARE(QAmqpClientPrivate::negotiateFrameMax(clientFrameMax, serverFrameMax), expectedFrameMax);
}

void tst_QAMQPClient::heartbeatNegotiation_data()
{
    QTest::addColumn<quint16>("clientHeartbeat");
    QTest::addColumn<quint16>("serverHeartbeat");
    QTest::addColumn<quint16>("expectedHeartbeat");

    QTest::newRow("both-disabled") << quint16(0) << quint16(0) << quint16(0);
    QTest::newRow("client-disabled") << quint16(0) << quint16(60) << quint16(60);
    QTest::newRow("server-disabled") << quint16(60) << quint16(0) << quint16(60);
    QTest::newRow("client-lower") << quint16(30) << quint16(60) << quint16(30);
    QTest::newRow("server-lower") << quint16(600) << quint16(60) << quint16(60);
    QTest::newRow("client-above-signed-range") << quint16(32768) << quint16(65535) << quint16(32768);
    QTest::newRow("server-above-signed-range") << quint16(65535) << quint16(32768) << quint16(32768);
    QTest::newRow("protocol-maximum") << quint16(65535) << quint16(65535) << quint16(65535);
}

void tst_QAMQPClient::heartbeatNegotiation()
{
    QFETCH(quint16, clientHeartbeat);
    QFETCH(quint16, serverHeartbeat);
    QFETCH(quint16, expectedHeartbeat);

    QCOMPARE(QAmqpClientPrivate::negotiateHeartbeat(clientHeartbeat, serverHeartbeat), expectedHeartbeat);
}

void tst_QAMQPClient::protocolMaximums()
{
    QAmqpClient client;
    client.setChannelMax(65535);
    client.setHeartbeatDelay(65535);
    QCOMPARE(client.channelMax(), qint32(65535));
    QCOMPARE(client.heartbeatDelay(), qint32(65535));

    client.setChannelMax(-1);
    client.setHeartbeatDelay(-1);
    QCOMPARE(client.channelMax(), qint32(65535));
    QCOMPARE(client.heartbeatDelay(), qint32(65535));

    client.setChannelMax(65536);
    client.setHeartbeatDelay(65536);
    QCOMPARE(client.channelMax(), qint32(65535));
    QCOMPARE(client.heartbeatDelay(), qint32(65535));
}

void tst_QAMQPClient::clientPropertiesMerge()
{
    QAmqpTable defaults;
    defaults.insert("version", "0.6.0");
    defaults.insert("platform", "Qt");
    QAmqpTable custom;
    custom.insert("version", "custom-version");
    custom.insert("operator", "MFT");

    const QAmqpTable merged = QAmqpClientPrivate::mergeClientProperties(defaults, custom);
    QCOMPARE(merged.value("version").toString(), QString("custom-version"));
    QCOMPARE(merged.value("platform").toString(), QString("Qt"));
    QCOMPARE(merged.value("operator").toString(), QString("MFT"));
}

void tst_QAMQPClient::writeQueueReservation_data()
{
    QTest::addColumn<qint64>("bufferedBytes");
    QTest::addColumn<qint64>("requestedBytes");
    QTest::addColumn<qint64>("maximumBytes");
    QTest::addColumn<bool>("accepted");

    QTest::newRow("empty") << qint64(0) << qint64(0) << qint64(10) << true;
    QTest::newRow("exact-limit") << qint64(6) << qint64(4) << qint64(10) << true;
    QTest::newRow("one-byte-over") << qint64(6) << qint64(5) << qint64(10) << false;
    QTest::newRow("already-over-limit") << qint64(11) << qint64(0) << qint64(10) << false;
    QTest::newRow("negative-request") << qint64(0) << qint64(-1) << qint64(10) << false;
    QTest::newRow("zero-budget") << qint64(0) << qint64(1) << qint64(0) << false;
}

void tst_QAMQPClient::writeQueueReservation()
{
    QFETCH(qint64, bufferedBytes);
    QFETCH(qint64, requestedBytes);
    QFETCH(qint64, maximumBytes);
    QFETCH(bool, accepted);

    QCOMPARE(QAmqpClientPrivate::canReserveWriteBytes(bufferedBytes, requestedBytes,
                                                      maximumBytes), accepted);
}

void tst_QAMQPClient::channelAllocationLimit()
{
    QAmqpClient client;
    client.setChannelMax(2);

    QAmqpExchange *exchange = client.createExchange("channel-limit-exchange");
    QAmqpQueue *queue = client.createQueue("channel-limit-queue");
    QVERIFY(exchange);
    QVERIFY(queue);
    QCOMPARE(exchange->channelNumber(), 1);
    QCOMPARE(queue->channelNumber(), 2);
    QVERIFY(!client.createQueue("channel-limit-overflow"));
    QVERIFY(!client.createExchange("channel-limit-explicit", 3));
    QVERIFY(!client.createExchange("channel-limit-zero", 0));
}

void tst_QAMQPClient::socketError()
{
    QAmqpClient client;
    client.connectToHost("amqp://127.0.0.1:56725/");
    QVERIFY(waitForSignal(&client, SIGNAL(socketErrorOccurred(QAbstractSocket::SocketError))));
    QCOMPARE(client.socketError(), QAbstractSocket::ConnectionRefusedError);
}

void tst_QAMQPClient::invalidUri_data()
{
    QTest::addColumn<QString>("uri");

    QTest::newRow("unsupported-scheme") << "http://localhost";
    QTest::newRow("missing-host") << "amqp:///vhost";
    QTest::newRow("empty") << "amqp://";
    QTest::newRow("empty2") << "amqp://:@/";
    QTest::newRow("onlyuser") << "amqp://user@";
    QTest::newRow("userpass") << "amqp://user:pass@";
    QTest::newRow("onlyport") << "amqp://:10000";
}

void tst_QAMQPClient::invalidUri()
{
    QFETCH(QString, uri);

    QAmqpClient client;
    QSignalSpy errors(&client, SIGNAL(error(QAMQP::Error)));
    client.connectToHost(uri);

    QCOMPARE(errors.count(), 1);
    QCOMPARE(client.error(), QAMQP::SyntaxError);
    QCOMPARE(client.errorString(), QLatin1String("Invalid AMQP URI: expected amqp:// or amqps:// with a host"));
    QCOMPARE(client.socketState(), QAbstractSocket::UnconnectedState);
}

void tst_QAMQPClient::validateUri_data()
{
    QTest::addColumn<QString>("uri");
    QTest::addColumn<QString>("expectedUsername");
    QTest::addColumn<QString>("expectedPassword");
    QTest::addColumn<QString>("expectedHost");
    QTest::addColumn<quint16>("expectedPort");
    QTest::addColumn<QString>("expectedVirtualHost");

    QTest::newRow("default vhost") << "amqp://guest:guest@192.168.1.10:5672/"
        << "guest" << "guest" << "192.168.1.10" << quint16(5672) << "/";
    QTest::newRow("standard") << "amqp://user:pass@host:10000/vhost"
        << "user" << "pass" << "host" << quint16(10000) << "vhost";
    QTest::newRow("urlencoded") << "amqp://user%61:%61pass@ho%61st:10000/v%2fhost"
        << "usera" << "apass" << "hoast" << quint16(10000) << "v/host";
    QTest::newRow("onlyhost") << "amqp://host" << "" << "" << "host" << quint16(AMQP_PORT) << "";
    QTest::newRow("urlencodedvhost") << "amqp://host/%2f"
        << "" << "" << "host" << quint16(AMQP_PORT) << "/";
    QTest::newRow("ipv6") << "amqp://[::1]" << "" << "" << "::1" << quint16(AMQP_PORT) << "";
}

void tst_QAMQPClient::validateUri()
{
    QFETCH(QString, uri);
    QFETCH(QString, expectedUsername);
    QFETCH(QString, expectedPassword);
    QFETCH(QString, expectedHost);
    QFETCH(quint16, expectedPort);
    QFETCH(QString, expectedVirtualHost);

    QAmqpClientPrivate clientPrivate(0);
    // fake init
    clientPrivate.authenticator = QSharedPointer<QAmqpAuthenticator>(
        new QAmqpPlainAuthenticator(QString(), QString()));

    // test parsing
    QVERIFY(clientPrivate.parseConnectionString(uri));
    QAmqpPlainAuthenticator *auth =
        static_cast<QAmqpPlainAuthenticator*>(clientPrivate.authenticator.data());

    QCOMPARE(auth->login(), expectedUsername);
    QCOMPARE(auth->password(), expectedPassword);
    QCOMPARE(clientPrivate.host, expectedHost);
    QCOMPARE(clientPrivate.port, expectedPort);
    QCOMPARE(clientPrivate.virtualHost, expectedVirtualHost);
}

void tst_QAMQPClient::issue38_helper(QAmqpClient *client)
{
    // connect
    client->setUsername("guest");
    client->setPassword("guest");
    client->connectToHost();
    QVERIFY(waitForSignal(client, SIGNAL(connected())));

    // create then declare, remove and close queue
    QAmqpQueue *queue = client->createQueue();
    QVERIFY(waitForSignal(queue, SIGNAL(opened())));
    queue->declare();
    QVERIFY(waitForSignal(queue, SIGNAL(declared())));
    queue->remove(QAmqpExchange::roForce);
    QVERIFY(waitForSignal(queue, SIGNAL(removed())));
    queue->close();
    QVERIFY(waitForSignal(queue, SIGNAL(closed())));
    queue->deleteLater();

    // disconnect
    client->disconnectFromHost();
    QVERIFY(waitForSignal(client, SIGNAL(disconnected())));
}

void tst_QAMQPClient::issue38()
{
    QAmqpClient client;
    issue38_helper(&client);
    issue38_helper(&client);
}

void tst_QAMQPClient::issue38_take2()
{
    QAmqpClient client;
    client.setUsername("guest");
    client.setPassword("guest");
    client.connectToHost();
    QVERIFY(waitForSignal(&client, SIGNAL(connected())));
    QAmqpExchange *exchange = client.createExchange("myexchange");
    exchange->declare(QAmqpExchange::Topic);
    QVERIFY(waitForSignal(exchange, SIGNAL(declared())));
    QAmqpQueue *queue = client.createQueue();
    queue->declare();
    QVERIFY(waitForSignal(queue, SIGNAL(declared())));
    queue->bind(exchange, "routingKeyin");
    QVERIFY(waitForSignal(queue, SIGNAL(bound())));
    queue->consume(QAmqpQueue::coNoAck);
    QVERIFY(waitForSignal(queue, SIGNAL(consuming(QString))));
    exchange->publish("test message", "routingKeyout");

    // delete everything
    queue->unbind(exchange, "routingKeyin");
    QVERIFY(waitForSignal(queue, SIGNAL(unbound())));
    queue->close();
    QVERIFY(waitForSignal(queue, SIGNAL(closed())));
    queue->deleteLater();

    exchange->close();
    QVERIFY(waitForSignal(exchange, SIGNAL(closed())));
    exchange->deleteLater();

    client.disconnectFromHost();
    QVERIFY(waitForSignal(&client,SIGNAL(disconnected())));

    // repeat the connection and create again here
    client.connectToHost();
    QVERIFY(waitForSignal(&client, SIGNAL(connected())));
    exchange = client.createExchange("myexchange");
    exchange->declare(QAmqpExchange::Topic);
    QVERIFY(waitForSignal(exchange, SIGNAL(declared())));
    queue = client.createQueue();
    queue->declare();
    QVERIFY(waitForSignal(queue, SIGNAL(declared())));
    queue->bind(exchange, "routingKeyin");
    QVERIFY(waitForSignal(queue, SIGNAL(bound())));
    queue->consume(QAmqpQueue::coNoAck);
    QVERIFY(waitForSignal(queue, SIGNAL(consuming(QString))));
    exchange->publish("test message", "routingKeyout");
    client.disconnectFromHost();
    QVERIFY(waitForSignal(&client,SIGNAL(disconnected())));
}


QTEST_MAIN(tst_QAMQPClient)
#include "tst_qamqpclient.moc"
