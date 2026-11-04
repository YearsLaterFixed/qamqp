#include <QtTest/QtTest>
#include <QTcpServer>
#include <QTcpSocket>
#include <QSslSocket>

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