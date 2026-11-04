#include <QTimer>
#include <QBuffer>
#include <QTextStream>
#include <QStringList>
#include <QSslSocket>
#include <QtEndian>
#include <limits.h>

#include "qamqpglobal.h"
#include "qamqpexchange.h"
#include "qamqpexchange_p.h"
#include "qamqpqueue.h"
#include "qamqpqueue_p.h"
#include "qamqpauthenticator.h"
#include "qamqptable.h"
#include "qamqpclient_p.h"
#include "qamqpclient.h"

QAmqpClientPrivate::QAmqpClientPrivate(QAmqpClient *q)
    : port(AMQP_PORT),
      host(AMQP_HOST),
      virtualHost(AMQP_VHOST),
      autoReconnect(false),
      reconnectFixedTimeout(false),
      timeout(0),
      connecting(false),
      useSsl(false),
      socket(0),
    bufferedWriteBytes(0),
    socketPendingBytes(0),
    maxWriteQueueBytes(16 * 1024 * 1024),
    bufferedControlBytes(0),
    nextWriteSequence(0),
      closed(false),
      connected(false),
      channelMax(0),
      nextChannelNumber(0),
      heartbeatDelay(0),
      frameMax(AMQP_FRAME_MAX),
      error(QAMQP::NoError),
      q_ptr(q)
{
    qRegisterMetaType<QAmqpMessage::PropertyHash>();
}

QAmqpClientPrivate::~QAmqpClientPrivate()
{
}

void QAmqpClientPrivate::init()
{
    Q_Q(QAmqpClient);
    initSocket();
    heartbeatTimer = new QTimer(q);
    QObject::connect(heartbeatTimer, SIGNAL(timeout()), q, SLOT(_q_heartbeat()));
    receiveHeartbeatTimer = new QTimer(q);
    QObject::connect(receiveHeartbeatTimer, &QTimer::timeout, q, [this] {
        _q_checkHeartbeat();
    });
    controlWriteTimer = new QTimer(q);
    controlWriteTimer->setSingleShot(true);
    QObject::connect(controlWriteTimer, &QTimer::timeout, q, [this] {
        failControlWrite("AMQP control write timed out");
    });
    reconnectTimer = new QTimer(q);
    reconnectTimer->setSingleShot(true);
    QObject::connect(reconnectTimer, SIGNAL(timeout()), q, SLOT(_q_connect()));

    // no default credentials: callers must explicitly configure a username/password
    // (or a custom QAmqpAuthenticator) before connecting
    authenticator = QSharedPointer<QAmqpAuthenticator>(
        new QAmqpPlainAuthenticator(QString(), QString()));
}

void QAmqpClientPrivate::initSocket()
{
    Q_Q(QAmqpClient);
    socket = new QSslSocket(q);
    socket->setSocketOption(QAbstractSocket::LowDelayOption, 1);
    socket->setSocketOption(QAbstractSocket::KeepAliveOption, 1);
    QObject::connect(socket, SIGNAL(connected()), q, SLOT(_q_socketConnected()));
    QObject::connect(socket, SIGNAL(disconnected()), q, SLOT(_q_socketDisconnected()));
    QObject::connect(socket, SIGNAL(readyRead()), q, SLOT(_q_readyRead()));
    QObject::connect(socket, &QIODevice::bytesWritten, q,
                     [this](qint64 bytes) { _q_bytesWritten(bytes); });
#if QT_VERSION >= 0x060000
    QObject::connect(socket,
                     SIGNAL(errorOccurred(QAbstractSocket::SocketError)),
                     q,
                     SLOT(_q_socketError(QAbstractSocket::SocketError)));
    QObject::connect(socket,
                     SIGNAL(errorOccurred(QAbstractSocket::SocketError)),
                     q,
                     SIGNAL(socketErrorOccurred(QAbstractSocket::SocketError)));
#else
    QObject::connect(socket,
                     SIGNAL(error(QAbstractSocket::SocketError)),
                     q,
                     SLOT(_q_socketError(QAbstractSocket::SocketError)));
    QObject::connect(socket,
                     SIGNAL(error(QAbstractSocket::SocketError)),
                     q,
                     SIGNAL(socketErrorOccurred(QAbstractSocket::SocketError)));
#endif
    QObject::connect(socket, SIGNAL(stateChanged(QAbstractSocket::SocketState)),
                          q, SIGNAL(socketStateChanged(QAbstractSocket::SocketState)));
    QObject::connect(socket, SIGNAL(sslErrors(QList<QSslError>)),
                          q, SIGNAL(sslErrors(QList<QSslError>)));
}

void QAmqpClientPrivate::resetChannelState()
{
    foreach (QString exchangeName, exchanges.channels()) {
      QAmqpExchange *exchange =
        qobject_cast<QAmqpExchange*>(exchanges.get(exchangeName));
      if (exchange) exchange->d_ptr->resetInternalState();
    }

    foreach (QString queueName, queues.channels()) {
      QAmqpQueue *queue =
        qobject_cast<QAmqpQueue*>(queues.get(queueName));
      if (queue) queue->d_ptr->resetInternalState();
    }
}

void QAmqpClientPrivate::setUsername(const QString &username)
{
    QAmqpAuthenticator *auth = authenticator.data();
    if (auth && auth->type() == QLatin1String("AMQPLAIN")) {
        QAmqpPlainAuthenticator *a = static_cast<QAmqpPlainAuthenticator*>(auth);
        a->setLogin(username);
    }
}

void QAmqpClientPrivate::setPassword(const QString &password)
{
    QAmqpAuthenticator *auth = authenticator.data();
    if (auth && auth->type() == QLatin1String("AMQPLAIN")) {
        QAmqpPlainAuthenticator *a = static_cast<QAmqpPlainAuthenticator*>(auth);
        a->setPassword(password);
    }
}

quint16 QAmqpClientPrivate::allocateChannelNumber(int requestedChannelNumber)
{
    if (requestedChannelNumber < -1 || requestedChannelNumber > USHRT_MAX)
        return 0;

    quint16 channelNumber = 0;
    if (requestedChannelNumber == -1) {
        if (nextChannelNumber == USHRT_MAX)
            return 0;
        channelNumber = nextChannelNumber + 1;
    } else {
        channelNumber = quint16(requestedChannelNumber);
    }

    if (channelMax && channelNumber > channelMax)
        return 0;

    nextChannelNumber = qMax(channelNumber, nextChannelNumber);
    return channelNumber;
}

quint16 QAmqpClientPrivate::negotiateChannelMax(quint16 clientChannelMax, quint16 serverChannelMax)
{
    if (!clientChannelMax)
        return serverChannelMax;
    if (!serverChannelMax)
        return clientChannelMax;
    return qMin(clientChannelMax, serverChannelMax);
}

qint32 QAmqpClientPrivate::negotiateFrameMax(qint32 clientFrameMax, qint32 serverFrameMax)
{
    qint32 negotiated = 0;
    if (!clientFrameMax)
        negotiated = serverFrameMax;
    else if (!serverFrameMax)
        negotiated = clientFrameMax;
    else
        negotiated = qMin(clientFrameMax, serverFrameMax);

    // zero keeps the "no limit" meaning; anything else must honour the AMQP floor
    if (negotiated && negotiated < AMQP_FRAME_MIN_SIZE)
        negotiated = AMQP_FRAME_MIN_SIZE;

    return negotiated;
}

quint16 QAmqpClientPrivate::negotiateHeartbeat(quint16 clientHeartbeat, quint16 serverHeartbeat)
{
    if (!clientHeartbeat)
        return serverHeartbeat;
    if (!serverHeartbeat)
        return clientHeartbeat;
    return qMin(clientHeartbeat, serverHeartbeat);
}

QAmqpTable QAmqpClientPrivate::mergeClientProperties(const QAmqpTable &defaults,
                                                      const QAmqpTable &custom)
{
    QAmqpTable properties = defaults;
    properties.insert(custom);
    return properties;
}

bool QAmqpClientPrivate::validateMethodArguments(const QAmqpMethodFrame &frame)
{
    QDataStream stream(frame.arguments());
    const auto readString = [&stream](QAmqpMetaType::ValueType type, bool required) {
        const QVariant value = QAmqpFrame::readAmqpField(stream, type);
        return value.isValid() && (!required || !value.toString().isEmpty());
    };
    const auto readBits = [&stream](quint8 allowed) {
        quint8 bits = 0;
        stream >> bits;
        return (bits & ~allowed) == 0;
    };
    const auto readClose = [&stream, &readString]() {
        quint16 code = 0, methodClass = 0, method = 0;
        stream >> code;
        const bool validText = readString(QAmqpMetaType::ShortString, false);
        stream >> methodClass >> method;
        return code != 0 && validText;
    };
    bool valid = false;
    switch (frame.methodClass()) {
    case QAmqpFrame::Connection:
        switch (frame.id()) {
        case miStart: {
            quint8 major = 0, minor = 0;
            stream >> major >> minor;
            QAmqpTable properties;
            stream >> properties;
            valid = major == 0 && minor == 9 &&
                readString(QAmqpMetaType::LongString, true) &&
                readString(QAmqpMetaType::LongString, true);
            break;
        }
        case miSecure:
            valid = readString(QAmqpMetaType::LongString, false);
            break;
        case miTune: {
            quint16 channels = 0, heartbeat = 0;
            quint32 maximumFrame = 0;
            stream >> channels >> maximumFrame >> heartbeat;
            valid = !maximumFrame || maximumFrame >= AMQP_FRAME_MIN_SIZE;
            break;
        }
        case miOpenOk:
            valid = readString(QAmqpMetaType::ShortString, false);
            break;
        case miClose:
            valid = readClose();
            break;
        case miCloseOk:
            valid = true;
            break;
        }
        break;
    case QAmqpFrame::Channel:
        switch (frame.id()) {
        case QAmqpChannelPrivate::miOpenOk:
            valid = readString(QAmqpMetaType::LongString, false);
            break;
        case QAmqpChannelPrivate::miFlow:
        case QAmqpChannelPrivate::miFlowOk:
            valid = readBits(0x01);
            break;
        case QAmqpChannelPrivate::miClose:
            valid = readClose();
            break;
        case QAmqpChannelPrivate::miCloseOk:
            valid = true;
            break;
        }
        break;
    case QAmqpFrame::Exchange:
        valid = frame.id() == QAmqpExchangePrivate::miDeclareOk ||
            frame.id() == QAmqpExchangePrivate::miDeleteOk;
        break;
    case QAmqpFrame::Queue:
        switch (frame.id()) {
        case QAmqpQueuePrivate::miDeclareOk: {
            valid = readString(QAmqpMetaType::ShortString, true);
            quint32 messages = 0, consumers = 0;
            stream >> messages >> consumers;
            break;
        }
        case QAmqpQueuePrivate::miPurgeOk:
        case QAmqpQueuePrivate::miDeleteOk: {
            quint32 messages = 0;
            stream >> messages;
            valid = true;
            break;
        }
        case QAmqpQueuePrivate::miBindOk:
        case QAmqpQueuePrivate::miUnbindOk:
            valid = true;
            break;
        }
        break;
    case QAmqpFrame::Basic:
        switch (frame.id()) {
        case QAmqpChannelPrivate::bmQosOk:
        case QAmqpChannelPrivate::bmRecoverOk:
            valid = true;
            break;
        case QAmqpChannelPrivate::bmConsumeOk:
        case QAmqpChannelPrivate::bmCancelOk:
            valid = readString(QAmqpMetaType::ShortString, true);
            break;
        case QAmqpChannelPrivate::bmReturn: {
            quint16 code = 0;
            stream >> code;
            valid = code != 0 && readString(QAmqpMetaType::ShortString, false) &&
                readString(QAmqpMetaType::ShortString, false) &&
                readString(QAmqpMetaType::ShortString, false);
            break;
        }
        case QAmqpChannelPrivate::bmDeliver:
        case QAmqpChannelPrivate::bmGetOk: {
            valid = frame.id() != QAmqpChannelPrivate::bmDeliver ||
                readString(QAmqpMetaType::ShortString, true);
            qulonglong deliveryTag = 0;
            stream >> deliveryTag;
            valid = valid && deliveryTag != 0 && readBits(0x01) &&
                readString(QAmqpMetaType::ShortString, false) &&
                readString(QAmqpMetaType::ShortString, false);
            if (frame.id() == QAmqpChannelPrivate::bmGetOk) {
                quint32 messages = 0;
                stream >> messages;
            }
            break;
        }
        case QAmqpChannelPrivate::bmGetEmpty:
            valid = readString(QAmqpMetaType::ShortString, false);
            break;
        case QAmqpChannelPrivate::bmAck:
        case QAmqpChannelPrivate::bmNack: {
            qulonglong deliveryTag = 0;
            quint8 bits = 0;
            stream >> deliveryTag >> bits;
            const quint8 allowed = frame.id() == QAmqpChannelPrivate::bmNack ? 0x03 : 0x01;
            valid = !(bits & ~allowed) && (deliveryTag != 0 || (bits & 0x01));
            break;
        }
        }
        break;
    case QAmqpFrame::Confirm:
        valid = frame.id() == QAmqpExchangePrivate::cmConfirmOk;
        break;
    case QAmqpFrame::Tx:
        valid = frame.id() == 11 || frame.id() == 21 || frame.id() == 31;
        break;
    }
    return valid && stream.status() == QDataStream::Ok && stream.atEnd();
}

bool QAmqpClientPrivate::validateDecodedFrame(const QAmqpFrame &frame,
                                               QDataStream::Status streamStatus,
                                               qint64 wirePayloadSize)
{
    if (streamStatus != QDataStream::Ok || wirePayloadSize < 0 ||
        wirePayloadSize > AMQP_FRAME_MAX)
        return false;

    switch (frame.type()) {
    case QAmqpFrame::Method: {
        const QAmqpMethodFrame &method = static_cast<const QAmqpMethodFrame &>(frame);
        if (method.size() != wirePayloadSize || method.size() < 4 || !validateMethodArguments(method))
            return false;

        switch (method.methodClass()) {
        case QAmqpFrame::Connection:
            return method.channel() == 0;
        case QAmqpFrame::Channel:
        case QAmqpFrame::Exchange:
        case QAmqpFrame::Queue:
        case QAmqpFrame::Basic:
        case QAmqpFrame::Confirm:
        case QAmqpFrame::Tx:
            return method.channel() > 0;
        }
        return false;
    }
    case QAmqpFrame::Header: {
        const QAmqpContentFrame &content = static_cast<const QAmqpContentFrame &>(frame);
        return wirePayloadSize >= 14 &&
               content.methodClass() == QAmqpFrame::Basic && content.channel() > 0;
    }
    case QAmqpFrame::Body: {
        const QAmqpContentBodyFrame &body = static_cast<const QAmqpContentBodyFrame &>(frame);
        return body.size() == wirePayloadSize && body.channel() > 0;
    }
    case QAmqpFrame::Heartbeat:
        return wirePayloadSize == 0 && frame.channel() == 0;
    }

    return false;
}

bool QAmqpClientPrivate::parseConnectionString(const QString &uri)
{
    QUrl connectionString = QUrl::fromUserInput(uri);

    if (!connectionString.isValid() ||
        (connectionString.scheme() != AMQP_SCHEME &&
         connectionString.scheme() != AMQP_SSL_SCHEME) ||
        connectionString.host().isEmpty()) {
        return false;
    }

    useSsl = (connectionString.scheme() == AMQP_SSL_SCHEME);
    port = connectionString.port((useSsl ? AMQP_SSL_PORT : AMQP_PORT));
    host = connectionString.host();

    QString vhost = connectionString.path();
    if (vhost.startsWith("/") && vhost.size() > 1)
        vhost = vhost.mid(1);
    virtualHost = vhost;
    setPassword(connectionString.password());
    setUsername(connectionString.userName());
    return true;
}

void QAmqpClientPrivate::_q_connect()
{
    if (reconnectTimer)
        reconnectTimer->stop();
    if (socket->state() != QAbstractSocket::UnconnectedState) {
        qAmqpDebug() << Q_FUNC_INFO << "socket already connected, disconnecting..";
        _q_disconnect();
        // We need to explicitly close connection here because either way it will not be closed until we receive closeOk
        closeConnection();
    }

    if (const QAmqpPlainAuthenticator *a = dynamic_cast<const QAmqpPlainAuthenticator*>(authenticator.data())) {
        if (a->login().isEmpty()) {
            qAmqpDebug() << Q_FUNC_INFO
                         << "connecting with no username/password configured; call setUsername()/setPassword() "
                            "(or setAuth()) explicitly - there is no default guest/guest credential anymore";
        }
    }

    qAmqpDebug() << "connecting to host: " << host << ", port: " << port;
    if (useSsl)
        socket->connectToHostEncrypted(host, port);
    else
        socket->connectToHost(host, port);
}

void QAmqpClientPrivate::_q_disconnect()
{
    stopHeartbeatTimers();
    if (reconnectTimer)
        reconnectTimer->stop();
    if (socket->state() == QAbstractSocket::UnconnectedState) {
        qAmqpDebug() << Q_FUNC_INFO << "already disconnected";
        return;
    }

    buffer.clear();
    close(200, "client disconnect");
}

// private slots
void QAmqpClientPrivate::_q_socketConnected()
{
    stopHeartbeatTimers();
    closed = false;
    if (reconnectTimer)
        reconnectTimer->stop();
    if(reconnectFixedTimeout == false)
        timeout = 0;
    char header[8] = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};
    enqueueWrite(QByteArray(header, 8), 0, true, false);
}

void QAmqpClientPrivate::_q_socketDisconnected()
{
    stopHeartbeatTimers();
    pendingChannelCloses.clear();
    Q_Q(QAmqpClient);
    buffer.clear();
    const qint64 discardedBytes = socketPendingBytes;
    pendingWrites.clear();
    controlWrites.clear();
    inFlightWrites.clear();
    bufferedControlBytes = 0;
    if (controlWriteTimer)
        controlWriteTimer->stop();
    socketPendingBytes = 0;
    releaseWriteBytes(discardedBytes);
    if (discardedBytes > 0)
        Q_EMIT q->writeQueueDiscarded(discardedBytes);
    resetChannelState();
    if (connected)
        connected = false;
    Q_EMIT q->disconnected();
}

void QAmqpClientPrivate::_q_heartbeat()
{
    QAmqpHeartbeatFrame frame;
    sendFrame(frame);
}

void QAmqpClientPrivate::stopHeartbeatTimers()
{
    if (heartbeatTimer)
        heartbeatTimer->stop();
    if (receiveHeartbeatTimer)
        receiveHeartbeatTimer->stop();
    lastReceived.invalidate();
}

void QAmqpClientPrivate::_q_checkHeartbeat()
{
    if (!heartbeatDelay || closed || socket->state() != QAbstractSocket::ConnectedState) {
        stopHeartbeatTimers();
        return;
    }
    if (!lastReceived.isValid())
        return;
    if (lastReceived.elapsed() < qint64(heartbeatDelay) * 1000)
        return;
    stopHeartbeatTimers();
    Q_Q(QAmqpClient);
    error = QAMQP::ConnectionForcedError;
    errorString = "AMQP receive heartbeat timeout";
    QPointer<QAmqpClient> guard(q);
    socket->abort();
    if (!guard)
        return;
    if (autoReconnect && reconnectTimer && !reconnectTimer->isActive())
        reconnectTimer->start(timeout > 0 ? timeout : 1000);
    Q_EMIT q->error(error);
}

void QAmqpClientPrivate::_q_socketError(QAbstractSocket::SocketError error)
{
    if(reconnectFixedTimeout == false)
    {
        if (timeout <= 0) {
            timeout = 1000;
        } else {
            if (timeout < 120000)
                timeout *= 5;
        }
    }

    switch (error) {
    case QAbstractSocket::ConnectionRefusedError:
    case QAbstractSocket::RemoteHostClosedError:
    case QAbstractSocket::SocketTimeoutError:
    case QAbstractSocket::NetworkError:
    case QAbstractSocket::ProxyConnectionClosedError:
    case QAbstractSocket::ProxyConnectionRefusedError:
    case QAbstractSocket::ProxyConnectionTimeoutError:

    default:
        qAmqpDebug() << "socket error: " << socket->errorString();
        break;
    }

    // per spec, on any error we need to close the socket immediately
    // and send no more data. only try to send the close message if we
    // are actively connected
    if (socket->state() == QAbstractSocket::ConnectedState ||
        socket->state() == QAbstractSocket::ConnectingState) {
        socket->abort();
    }

    errorString = socket->errorString();

    if (autoReconnect && reconnectTimer) {
        qAmqpDebug() << "trying to reconnect after: " << timeout << "ms";
        reconnectTimer->start(timeout);
    }
}

void QAmqpClientPrivate::_q_readyRead()
{
    Q_Q(QAmqpClient);
    if (lastReceived.isValid() && socket->bytesAvailable() > 0 && !closed)
        lastReceived.restart();
    
    while (socket->bytesAvailable() >= QAmqpFrame::HEADER_SIZE) {
        unsigned char headerData[QAmqpFrame::HEADER_SIZE];
        socket->peek((char*)headerData, QAmqpFrame::HEADER_SIZE);
        const quint32 payloadSize = qFromBigEndian<quint32>(headerData + 3);

        const qint64 readSize = QAmqpFrame::HEADER_SIZE + payloadSize + QAmqpFrame::FRAME_END_SIZE;
        if (Q_UNLIKELY(payloadSize > AMQP_FRAME_MAX || (frameMax && readSize > frameMax))) {
            close(QAMQP::FrameError, "frame size too large");
            return;
        }

        if (socket->bytesAvailable() < readSize)
            return;

        buffer.resize(readSize);
        socket->read(buffer.data(), readSize);
        const char *bufferData = buffer.constData();
        const quint8 type = *(quint8*)&bufferData[0];
        const quint8 magic = *(quint8*)&bufferData[QAmqpFrame::HEADER_SIZE + payloadSize];
        if (Q_UNLIKELY(magic != QAmqpFrame::FRAME_END)) {
            close(QAMQP::UnexpectedFrameError, "wrong end of frame");
            return;
        }

        QDataStream streamB(&buffer, QIODevice::ReadOnly);
        if (closed && type != QAmqpFrame::Method)
            continue;
        switch (static_cast<QAmqpFrame::FrameType>(type)) {
        case QAmqpFrame::Method:
        {
            QAmqpMethodFrame frame;
            streamB >> frame;
            if (closed) {
                if (streamB.status() == QDataStream::Ok && frame.channel() == 0 &&
                    frame.methodClass() == QAmqpFrame::Connection && frame.id() == miCloseOk &&
                    frame.arguments().isEmpty())
                    closeOk(frame);
                continue;
            }
            if (!validateDecodedFrame(frame, streamB.status(), payloadSize)) {
                closed = true;
                close(QAMQP::FrameError, "invalid method frame payload");
                return;
            }

            if (frame.methodClass() == QAmqpFrame::Connection) {
                _q_method(frame);
            } else {
                const QList<QAmqpMethodFrameHandler *> handlers =
                    methodHandlersByChannel.value(frame.channel());
                const bool expectedCloseReply = frame.methodClass() == QAmqpFrame::Channel &&
                    frame.id() == QAmqpChannelPrivate::miCloseOk && frame.arguments().isEmpty() &&
                    pendingChannelCloses.remove(frame.channel()) > 0;
                if (handlers.isEmpty()) {
                    if (expectedCloseReply)
                        break;
                    close(QAMQP::ChannelError, "method frame references unknown channel");
                    return;
                }
                foreach (QAmqpMethodFrameHandler *methodHandler, handlers)
                    methodHandler->_q_method(frame);
            }
        }
            break;
        case QAmqpFrame::Header:
        {
            QAmqpContentFrame frame;
            streamB >> frame;
            if (!validateDecodedFrame(frame, streamB.status(), payloadSize)) {
                close(QAMQP::FrameError, "invalid content header");
                return;
            }
            const QList<QAmqpContentFrameHandler *> handlers =
                contentHandlerByChannel.value(frame.channel());
            if (handlers.isEmpty()) {
                close(QAMQP::ChannelError, "channel number must be greater than zero");
                return;
            }

            foreach (QAmqpContentFrameHandler *methodHandler, handlers)
                methodHandler->_q_content(frame);
        }
            break;
        case QAmqpFrame::Body:
        {
            QAmqpContentBodyFrame frame;
            streamB >> frame;
            if (!validateDecodedFrame(frame, streamB.status(), payloadSize)) {
                close(QAMQP::FrameError, "invalid content body");
                return;
            }
            const QList<QAmqpContentBodyFrameHandler *> handlers =
                bodyHandlersByChannel.value(frame.channel());
            if (handlers.isEmpty()) {
                close(QAMQP::ChannelError, "channel number must be greater than zero");
                return;
            }

            foreach (QAmqpContentBodyFrameHandler *methodHandler, handlers)
                methodHandler->_q_body(frame);
        }
            break;
        case QAmqpFrame::Heartbeat:
        {
            QAmqpHeartbeatFrame frame;
            streamB >> frame;
            if (!validateDecodedFrame(frame, streamB.status(), payloadSize)) {
                close(QAMQP::FrameError, "invalid heartbeat frame");
                return;
            }

            qAmqpDebug("AMQP: Heartbeat");
            Q_EMIT q->heartbeat();
        }
            break;
        default:
            qAmqpDebug() << "AMQP: Unknown frame type: " << type;
            close(QAMQP::FrameError, "invalid frame type");
            return;
        }
    }
}

bool QAmqpClientPrivate::canReserveWriteBytes(qint64 bufferedBytes,
                                              qint64 requestedBytes,
                                              qint64 maximumBytes)
{
    return requestedBytes >= 0 && maximumBytes >= 0 &&
           bufferedBytes <= maximumBytes && requestedBytes <= maximumBytes - bufferedBytes;
}

bool QAmqpClientPrivate::reserveWriteBytes(qint64 bytes)
{
    if (!canReserveWriteBytes(bufferedWriteBytes, bytes, maxWriteQueueBytes)) {
        Q_Q(QAmqpClient);
        Q_EMIT q->writeQueueOverflow(bytes);
        return false;
    }
    bufferedWriteBytes += bytes;
    return true;
}

void QAmqpClientPrivate::releaseWriteBytes(qint64 bytes)
{
    bufferedWriteBytes = qMax(qint64(0), bufferedWriteBytes - qMax(qint64(0), bytes));
}

bool QAmqpClientPrivate::sendFrame(const QAmqpFrame &frame)
{
    QByteArray data;
    QBuffer buffer(&data);
    if (!buffer.open(QIODevice::WriteOnly))
        return false;
    QDataStream stream(&buffer);
    stream << frame;
    if (stream.status() != QDataStream::Ok) {
        Q_Q(QAmqpClient);
        error = QAMQP::FrameError;
        errorString = "AMQP frame contains an invalid field";
        Q_EMIT q->error(error);
        return false;
    }
    if (!enqueueWrite(data, 0, true, frame.type() == QAmqpFrame::Heartbeat))
        return false;
    if (frame.type() == QAmqpFrame::Method) {
        const QAmqpMethodFrame &method = static_cast<const QAmqpMethodFrame &>(frame);
        if (method.methodClass() == QAmqpFrame::Channel &&
            method.id() == QAmqpChannelPrivate::miClose)
            pendingChannelCloses.insert(method.channel());
    }
    return true;
}

bool QAmqpClientPrivate::sendFrameBatch(const QByteArray &data, qint64 reservedBytes)
{
    return enqueueWrite(data, reservedBytes, false, false);
}

bool QAmqpClientPrivate::enqueueWrite(const QByteArray &data, qint64 reservedBytes,
                                    bool control, bool heartbeat)
{
    if (socket->state() != QAbstractSocket::ConnectedState) {
        qAmqpDebug() << Q_FUNC_INFO << "socket not connected: " << socket->state();
        return false;
    }

    const qint64 actualBytes = data.size();
    if (!actualBytes)
        return false;
    if (control) {
        if (!canReserveWriteBytes(bufferedControlBytes, actualBytes, ControlWriteQueueBytes)) {
            failControlWrite("AMQP control write reserve exhausted");
            return false;
        }
        bufferedControlBytes += actualBytes;
    } else {
        if (reservedBytes < 0 || reservedBytes > bufferedWriteBytes)
            return false;
        const qint64 extraBytes = qMax(qint64(0), actualBytes - reservedBytes);
        if (extraBytes > 0 &&
            !canReserveWriteBytes(bufferedWriteBytes, extraBytes, maxWriteQueueBytes)) {
            Q_Q(QAmqpClient);
            Q_EMIT q->writeQueueOverflow(actualBytes);
            return false;
        }

        bufferedWriteBytes += extraBytes;
        if (reservedBytes > actualBytes)
            releaseWriteBytes(reservedBytes - actualBytes);
    }

    PendingWrite pending;
    pending.data = data;
    pending.offset = 0;
    pending.frameEnd = 0;
    pending.channel = data.size() >= QAmqpFrame::HEADER_SIZE
                          ? qFromBigEndian<quint16>(reinterpret_cast<const uchar *>(data.constData() + 1))
                          : 0;
    pending.sequence = nextWriteSequence++;
    pending.heartbeat = heartbeat;
    if (control) {
        controlWrites.enqueue(pending);
        if (controlWriteTimer && !controlWriteTimer->isActive())
            controlWriteTimer->start(heartbeatDelay ? qMin(30000, int(heartbeatDelay) * 1000) : 30000);
    } else {
        pendingWrites.enqueue(pending);
        socketPendingBytes += actualBytes;
    }
    flushPendingWrites();
    return !control || socket->state() == QAbstractSocket::ConnectedState;
}

bool QAmqpClientPrivate::canSendControl(int index) const
{
    const PendingWrite &control = controlWrites.at(index);
    if (control.offset)
        return true;
    for (int earlier = 0; earlier < index; ++earlier) {
        if (controlWrites.at(earlier).channel == control.channel)
            return false;
    }
    if (control.heartbeat)
        return true;
    foreach (const PendingWrite &publish, pendingWrites) {
        if (publish.sequence < control.sequence &&
            (!control.channel || publish.channel == control.channel))
            return false;
    }
    return true;
}

void QAmqpClientPrivate::failControlWrite(const QString &reason)
{
    Q_Q(QAmqpClient);
    error = QAMQP::ResourceError;
    errorString = reason;
    if (controlWriteTimer)
        controlWriteTimer->stop();
    socket->abort();
    if (autoReconnect && reconnectTimer)
        reconnectTimer->start(qMax(1000, timeout));
    Q_EMIT q->error(error);
}

void QAmqpClientPrivate::flushPendingWrites()
{
    if (socket->state() != QAbstractSocket::ConnectedState)
        return;

    const qint64 socketBufferLimit = 64 * 1024;
    while (!pendingWrites.isEmpty() || !controlWrites.isEmpty()) {
        const qint64 available = socketBufferLimit - socket->bytesToWrite();
        if (available <= 0)
            return;

        int controlIndex = -1;
        const bool partialApplicationFrame = !pendingWrites.isEmpty() &&
            pendingWrites.head().offset < pendingWrites.head().frameEnd;
        if (!partialApplicationFrame) {
            for (int index = 0; index < controlWrites.size(); ++index) {
                if (controlWrites.at(index).offset) {
                    controlIndex = index;
                    break;
                }
            }
            if (controlIndex < 0) {
                for (int index = 0; index < controlWrites.size(); ++index) {
                    if (canSendControl(index)) {
                        controlIndex = index;
                        break;
                    }
                }
            }
        }
        if (controlIndex < 0 && pendingWrites.isEmpty())
            return;
        PendingWrite &pending = controlIndex >= 0 ? controlWrites[controlIndex] : pendingWrites.head();
        if (pending.frameEnd == pending.offset) {
            pending.frameEnd = pending.data.size();
            const qint64 remainingBytes = pending.data.size() - pending.offset;
            if (remainingBytes >= QAmqpFrame::HEADER_SIZE) {
                const uchar *header = reinterpret_cast<const uchar *>(pending.data.constData() + pending.offset);
                const qint64 frameBytes = QAmqpFrame::HEADER_SIZE +
                    qFromBigEndian<quint32>(header + 3) + QAmqpFrame::FRAME_END_SIZE;
                if ((header[0] == QAmqpFrame::Method || header[0] == QAmqpFrame::Header ||
                     header[0] == QAmqpFrame::Body || header[0] == QAmqpFrame::Heartbeat) &&
                    frameBytes <= remainingBytes)
                    pending.frameEnd = pending.offset + frameBytes;
            }
        }
        const qint64 remaining = pending.frameEnd - pending.offset;
        const qint64 chunkSize = qMin(available, remaining);
        const qint64 written = socket->write(pending.data.constData() + pending.offset,
                                              chunkSize);
        if (written < 0) {
            failControlWrite("AMQP asynchronous socket write failed");
            return;
        }
        if (!written)
            return;

        const bool control = controlIndex >= 0;
        if (!inFlightWrites.isEmpty() && inFlightWrites.back().control == control) {
            inFlightWrites.back().bytes += written;
        } else {
            WriteSegment segment;
            segment.bytes = written;
            segment.control = control;
            inFlightWrites.enqueue(segment);
        }
        pending.offset += written;
        if (pending.offset == pending.data.size()) {
            if (control)
                controlWrites.removeAt(controlIndex);
            else
                pendingWrites.dequeue();
        }
    }
}

void QAmqpClientPrivate::_q_bytesWritten(qint64 bytes)
{
    bool controlProgress = false;
    while (bytes > 0 && !inFlightWrites.isEmpty()) {
        WriteSegment &segment = inFlightWrites.head();
        const qint64 completed = qMin(bytes, segment.bytes);
        if (segment.control) {
            bufferedControlBytes -= completed;
            controlProgress = true;
        } else {
            socketPendingBytes -= completed;
            releaseWriteBytes(completed);
        }
        segment.bytes -= completed;
        bytes -= completed;
        if (!segment.bytes)
            inFlightWrites.dequeue();
    }
    if (!bufferedControlBytes && controlWriteTimer)
        controlWriteTimer->stop();
    else if (controlProgress && controlWriteTimer)
        controlWriteTimer->start(heartbeatDelay ? qMin(30000, int(heartbeatDelay) * 1000) : 30000);
    flushPendingWrites();
    flushDeferredPublishes();
}

void QAmqpClientPrivate::flushDeferredPublishes()
{
    foreach (const QString &exchangeName, exchanges.channels()) {
        QAmqpExchange *exchange = qobject_cast<QAmqpExchange *>(exchanges.get(exchangeName));
        if (exchange)
            exchange->d_func()->flushPendingPublishes();
    }
}

void QAmqpClientPrivate::closeConnection()
{
    stopHeartbeatTimers();
    qAmqpDebug("AMQP: closing connection");

    connected = false;
    if (reconnectTimer)
        reconnectTimer->stop();
    socket->disconnectFromHost();
}

bool QAmqpClientPrivate::_q_method(const QAmqpMethodFrame &frame)
{
    Q_ASSERT(frame.methodClass() == QAmqpFrame::Connection);
    if (frame.methodClass() != QAmqpFrame::Connection)
        return false;

    if (closed) {
        if (frame.id() == QAmqpClientPrivate::miCloseOk)
            closeOk(frame);
        return false;
    }

    switch (QAmqpClientPrivate::MethodId(frame.id())) {
    case QAmqpClientPrivate::miStart:
        start(frame);
        break;
    case QAmqpClientPrivate::miSecure:
        secure(frame);
        break;
    case QAmqpClientPrivate::miTune:
        tune(frame);
        break;
    case QAmqpClientPrivate::miOpenOk:
        openOk(frame);
        break;
    case QAmqpClientPrivate::miClose:
        close(frame);
        break;
    case QAmqpClientPrivate::miCloseOk:
        closeOk(frame);
        break;
    default:
        qAmqpDebug("Unknown method-id %d", frame.id());
    }

    return true;
}

void QAmqpClientPrivate::start(const QAmqpMethodFrame &frame)
{
    QByteArray data = frame.arguments();
    QDataStream stream(&data, QIODevice::ReadOnly);

    quint8 version_major = 0;
    quint8 version_minor = 0;
    stream >> version_major >> version_minor;

    QAmqpTable table;
    stream >> table;

    QStringList mechanisms =
        QAmqpFrame::readAmqpField(stream, QAmqpMetaType::LongString).toString().split(' ');
    QString locales = QAmqpFrame::readAmqpField(stream, QAmqpMetaType::LongString).toString();

    qAmqpDebug("-> connection#start( version_major=%d, version_minor=%d, mechanisms=(%s), locales=%s )",
           version_major, version_minor, qPrintable(mechanisms.join(",")), qPrintable(locales));

    if (!mechanisms.contains(authenticator->type())) {
        socket->disconnectFromHost();
        return;
    }

    startOk();
}

void QAmqpClientPrivate::secure(const QAmqpMethodFrame &frame)
{
    Q_UNUSED(frame)
    qAmqpDebug("-> connection#secure()");
}

void QAmqpClientPrivate::tune(const QAmqpMethodFrame &frame)
{
    QByteArray data = frame.arguments();
    QDataStream stream(&data, QIODevice::ReadOnly);

    quint16 channel_max = 0,
           heartbeat_delay = 0;
    quint32 frame_max = 0;

    stream >> channel_max;
    stream >> frame_max;
    stream >> heartbeat_delay;

    const qint32 serverFrameMax = frame_max ? qint32(qMin(frame_max, quint32(AMQP_FRAME_MAX))) : 0;
    frameMax = negotiateFrameMax(frameMax, serverFrameMax);
    channelMax = negotiateChannelMax(channelMax, channel_max);
    heartbeatDelay = negotiateHeartbeat(heartbeatDelay, heartbeat_delay);

    qAmqpDebug("-> connection#tune( channel_max=%d, frame_max=%d, heartbeat=%d )",
               channelMax, frameMax, heartbeatDelay);

    stopHeartbeatTimers();
    if (heartbeatDelay) {
        lastReceived.start();
        if (heartbeatTimer)
            heartbeatTimer->start(int(heartbeatDelay) * 500);
        if (receiveHeartbeatTimer)
            receiveHeartbeatTimer->start(qMin(1000, qMax(100, int(heartbeatDelay) * 250)));
    }

    tuneOk();
    open();
}

void QAmqpClientPrivate::openOk(const QAmqpMethodFrame &frame)
{
    Q_Q(QAmqpClient);
    Q_UNUSED(frame)
    qAmqpDebug("-> connection#openOk()");
    connected = true;
    Q_EMIT q->connected();
}

void QAmqpClientPrivate::closeOk(const QAmqpMethodFrame &frame)
{
    Q_UNUSED(frame)
    qAmqpDebug("-> connection#closeOk()");
    closeConnection();
}

void QAmqpClientPrivate::close(const QAmqpMethodFrame &frame)
{
    Q_Q(QAmqpClient);
    QByteArray data = frame.arguments();
    QDataStream stream(&data, QIODevice::ReadOnly);
    qint16 code = 0, classId, methodId;
    stream >> code;
    QString text = QAmqpFrame::readAmqpField(stream, QAmqpMetaType::ShortString).toString();
    stream >> classId;
    stream >> methodId;

    qAmqpDebug("-> connection#close( reply-code=%d, reply-text=%s, class-id=%d, method-id:%d )",
               code, qPrintable(text), classId, methodId);

    QAMQP::Error checkError = QAMQP::errorFromCode(code);
    if (checkError != QAMQP::NoError) {
        error = checkError;
        errorString = qPrintable(text);
        Q_EMIT q->error(error);

        // if it was a force disconnect, simulate receiving a closeOk
        if (checkError == QAMQP::ConnectionForcedError) {
          closeConnection();
          if (autoReconnect) {
            qAmqpDebug() << "trying to reconnect after: " << timeout << "ms";
            QTimer::singleShot(timeout, q, SLOT(_q_connect()));
          }

          return;
        }
    }

    connected = false;
    Q_EMIT q->disconnected();

    // complete handshake
    QAmqpMethodFrame closeOkFrame(QAmqpFrame::Connection, QAmqpClientPrivate::miCloseOk);
    qAmqpDebug("<- connection#closeOk()");
    sendFrame(closeOkFrame);
    closeConnection();
}

void QAmqpClientPrivate::startOk()
{
    QAmqpMethodFrame frame(QAmqpFrame::Connection, QAmqpClientPrivate::miStartOk);
    QByteArray arguments;
    QDataStream stream(&arguments, QIODevice::WriteOnly);

    QAmqpTable defaultProperties;
    defaultProperties["version"] = QString(QAMQP_VERSION);
    defaultProperties["platform"] = QString("Qt %1").arg(qVersion());
    defaultProperties["product"] = QString("QAMQP");
    QAmqpTable clientProperties = mergeClientProperties(defaultProperties, customProperties);
    stream << clientProperties;

    authenticator->write(stream);
    QAmqpFrame::writeAmqpField(stream, QAmqpMetaType::ShortString, QLatin1String("en_US"));
    frame.setArguments(arguments, stream.status());

    qAmqpDebug("<- connection#startOk()");  // @todo: fill this out
    sendFrame(frame);
}

void QAmqpClientPrivate::secureOk()
{
    qAmqpDebug("-> connection#secureOk()");
}

void QAmqpClientPrivate::tuneOk()
{
    QAmqpMethodFrame frame(QAmqpFrame::Connection, QAmqpClientPrivate::miTuneOk);
    QByteArray arguments;
    QDataStream stream(&arguments, QIODevice::WriteOnly);

    stream << quint16(channelMax);
    stream << qint32(frameMax);
    stream << quint16(heartbeatDelay);

    qAmqpDebug("<- connection#tuneOk( channelMax=%d, frameMax=%d, heartbeatDelay=%d )",
               channelMax, frameMax, heartbeatDelay);

    frame.setArguments(arguments);
    sendFrame(frame);
}

void QAmqpClientPrivate::open()
{
    QAmqpMethodFrame frame(QAmqpFrame::Connection, QAmqpClientPrivate::miOpen);
    QByteArray arguments;
    QDataStream stream(&arguments, QIODevice::WriteOnly);

    QAmqpFrame::writeAmqpField(stream, QAmqpMetaType::ShortString, virtualHost);

    stream << qint8(0);
    stream << qint8(0);

    qAmqpDebug("<- connection#open( virtualHost=%s, reserved-1=%d, reserved-2=%d )",
               qPrintable(virtualHost), 0, 0);

    frame.setArguments(arguments, stream.status());
    sendFrame(frame);
}

void QAmqpClientPrivate::close(int code, const QString &text, int classId, int methodId)
{
    QByteArray arguments;
    QDataStream stream(&arguments, QIODevice::WriteOnly);
    stream << qint16(code);
    QAmqpFrame::writeAmqpField(stream, QAmqpMetaType::ShortString, text);
    stream << qint16(classId);
    stream << qint16(methodId);

    qAmqpDebug("<- connection#close( reply-code=%d, reply-text=%s, class-id=%d, method-id:%d )",
               code, qPrintable(text), classId, methodId);

    QAmqpMethodFrame frame(QAmqpFrame::Connection, QAmqpClientPrivate::miClose);
    frame.setArguments(arguments, stream.status());
    sendFrame(frame);
}

//////////////////////////////////////////////////////////////////////////

QAmqpClient::QAmqpClient(QObject *parent)
    : QObject(parent),
      d_ptr(new QAmqpClientPrivate(this))
{
    Q_D(QAmqpClient);
    d->init();
}

QAmqpClient::QAmqpClient(QAmqpClientPrivate *dd, QObject *parent)
    : QObject(parent),
      d_ptr(dd)
{
}

QAmqpClient::~QAmqpClient()
{
    Q_D(QAmqpClient);
    if (d->connected)
        d->_q_disconnect();
}

bool QAmqpClient::isConnected() const
{
    Q_D(const QAmqpClient);
    return d->connected;
}

quint16 QAmqpClient::port() const
{
    Q_D(const QAmqpClient);
    return d->port;
}

void QAmqpClient::setPort(quint16 port)
{
    Q_D(QAmqpClient);
    d->port = port;
}

QString QAmqpClient::host() const
{
    Q_D(const QAmqpClient);
    return d->host;
}

void QAmqpClient::setHost(const QString &host)
{
    Q_D(QAmqpClient);
    d->host = host;
}

QString QAmqpClient::virtualHost() const
{
    Q_D(const QAmqpClient);
    return d->virtualHost;
}

void QAmqpClient::setVirtualHost(const QString &virtualHost)
{
    Q_D(QAmqpClient);
    d->virtualHost = virtualHost;
}

QString QAmqpClient::username() const
{
    Q_D(const QAmqpClient);
    const QAmqpAuthenticator *auth = d->authenticator.data();
    if (auth && auth->type() == QLatin1String("AMQPLAIN")) {
        const QAmqpPlainAuthenticator *a = static_cast<const QAmqpPlainAuthenticator*>(auth);
        return a->login();
    }

    return QString();
}

void QAmqpClient::setUsername(const QString &username)
{
    Q_D(QAmqpClient);
    d->setUsername(username);
}

QString QAmqpClient::password() const
{
    Q_D(const QAmqpClient);
    const QAmqpAuthenticator *auth = d->authenticator.data();
    if (auth && auth->type() == QLatin1String("AMQPLAIN")) {
        const QAmqpPlainAuthenticator *a = static_cast<const QAmqpPlainAuthenticator*>(auth);
        return a->password();
    }

    return QString();
}

void QAmqpClient::setPassword(const QString &password)
{
    Q_D(QAmqpClient);
    d->setPassword(password);
}

QAmqpExchange *QAmqpClient::createExchange(int channelNumber)
{
    return createExchange(QString(), channelNumber);
}

QAmqpExchange *QAmqpClient::createExchange(const QString &name, int channelNumber)
{
    Q_D(QAmqpClient);
    QAmqpExchange *exchange;
    if (d->exchanges.contains(name)) {
        exchange = qobject_cast<QAmqpExchange*>(d->exchanges.get(name));
        if (exchange)
            return exchange;
    }

    exchange = new QAmqpExchange(channelNumber, this);
    if (!exchange->channelNumber()) {
        delete exchange;
        return 0;
    }
    d->methodHandlersByChannel[exchange->channelNumber()].append(exchange->d_func());
    connect(this, SIGNAL(connected()), exchange, SLOT(_q_open()));
    connect(this, SIGNAL(disconnected()), exchange, SLOT(_q_disconnected()));
    exchange->d_func()->open();

    if (!name.isEmpty())
        exchange->setName(name);
    d->exchanges.put(exchange);
    return exchange;
}

QAmqpQueue *QAmqpClient::createQueue(int channelNumber)
{
    return createQueue(QString(), channelNumber);
}

QAmqpQueue *QAmqpClient::createQueue(const QString &name, int channelNumber)
{
    Q_D(QAmqpClient);
    QAmqpQueue *queue;
    if (d->queues.contains(name)) {
        queue = qobject_cast<QAmqpQueue*>(d->queues.get(name));
        if (queue)
            return queue;
    }

    queue = new QAmqpQueue(channelNumber, this);
    if (!queue->channelNumber()) {
        delete queue;
        return 0;
    }
    d->methodHandlersByChannel[queue->channelNumber()].append(queue->d_func());
    d->contentHandlerByChannel[queue->channelNumber()].append(queue->d_func());
    d->bodyHandlersByChannel[queue->channelNumber()].append(queue->d_func());
    connect(this, SIGNAL(connected()), queue, SLOT(_q_open()));
    connect(this, SIGNAL(disconnected()), queue, SLOT(_q_disconnected()));
    queue->d_func()->open();

    if (!name.isEmpty())
        queue->setName(name);
    d->queues.put(queue);
    return queue;
}

void QAmqpClient::setAuth(QAmqpAuthenticator *authenticator)
{
    Q_D(QAmqpClient);
    d->authenticator = QSharedPointer<QAmqpAuthenticator>(authenticator);
}

QAmqpAuthenticator *QAmqpClient::auth() const
{
    Q_D(const QAmqpClient);
    return d->authenticator.data();
}

bool QAmqpClient::autoReconnect() const
{
    Q_D(const QAmqpClient);
    return d->autoReconnect;
}

void QAmqpClient::setAutoReconnect(bool value, int timeout)
{
    Q_D(QAmqpClient);
    d->autoReconnect = value;

    if((value == true) && (timeout > 0))
    {
        d->timeout = timeout;
        d->reconnectFixedTimeout = true;
    }
    else
    {
        d->timeout = 0;
        d->reconnectFixedTimeout = false;
    }
}

qint32 QAmqpClient::channelMax() const
{
    Q_D(const QAmqpClient);
    return d->channelMax;
}

void QAmqpClient::setChannelMax(qint32 channelMax)
{
    Q_D(QAmqpClient);
    if (d->connected) {
        qAmqpDebug() << Q_FUNC_INFO << "can't modify value while connected";
        return;
    }

    if (channelMax < 0 || channelMax > USHRT_MAX) {
        qAmqpDebug() << Q_FUNC_INFO << "channel-max must be in the range 0..65535";
        return;
    }

    d->channelMax = quint16(channelMax);
}

qint32 QAmqpClient::frameMax() const
{
    Q_D(const QAmqpClient);
    return d->frameMax;
}

void QAmqpClient::setFrameMax(qint32 frameMax)
{
    Q_D(QAmqpClient);
    if (d->connected) {
        qAmqpDebug() << Q_FUNC_INFO << "can't modify value while connected";
        return;
    }

    d->frameMax = qMax(frameMax, AMQP_FRAME_MIN_SIZE);
}

qint32 QAmqpClient::heartbeatDelay() const
{
    Q_D(const QAmqpClient);
    return d->heartbeatDelay;
}

void QAmqpClient::setHeartbeatDelay(qint32 delay)
{
    Q_D(QAmqpClient);
    if (d->connected) {
        qAmqpDebug() << Q_FUNC_INFO << "can't modify value while connected";
        return;
    }

    if (delay < 0 || delay > USHRT_MAX) {
        qAmqpDebug() << Q_FUNC_INFO << "heartbeat delay must be in the range 0..65535";
        return;
    }

    d->heartbeatDelay = quint16(delay);
}

qlonglong QAmqpClient::maxWriteQueueBytes() const
{
    Q_D(const QAmqpClient);
    return d->maxWriteQueueBytes;
}

void QAmqpClient::setMaxWriteQueueBytes(qlonglong bytes)
{
    Q_D(QAmqpClient);
    d->maxWriteQueueBytes = qMax(qlonglong(0), bytes);
}

qlonglong QAmqpClient::bufferedWriteBytes() const
{
    Q_D(const QAmqpClient);
    return d->bufferedWriteBytes;
}

int QAmqpClient::writeTimeout() const
{
    return QAmqpFrame::writeTimeout();
}

void QAmqpClient::setWriteTimeout(int msecs)
{
    QAmqpFrame::setWriteTimeout(msecs);
}

void QAmqpClient::addCustomProperty(const QString &name, const QString &value)
{
    Q_D(QAmqpClient);
    d->customProperties.insert(name, value);
}

QString QAmqpClient::customProperty(const QString &name) const
{
    Q_D(const QAmqpClient);
    return d->customProperties.value(name).toString();
}

QAbstractSocket::SocketError QAmqpClient::socketError() const
{
    Q_D(const QAmqpClient);
    return d->socket->error();
}

QAbstractSocket::SocketState QAmqpClient::socketState() const
{
    Q_D(const QAmqpClient);
    return d->socket->state();
}

QAMQP::Error QAmqpClient::error() const
{
    Q_D(const QAmqpClient);
    return d->error;
}

QString QAmqpClient::errorString() const
{
    Q_D(const QAmqpClient);
    return d->errorString;
}

QSslConfiguration QAmqpClient::sslConfiguration() const
{
    Q_D(const QAmqpClient);
    return d->socket->sslConfiguration();
}

void QAmqpClient::setSslConfiguration(const QSslConfiguration &config)
{
    Q_D(QAmqpClient);
    if (!config.isNull()) {
        d->useSsl = true;
        d->port = AMQP_SSL_PORT;
        d->socket->setSslConfiguration(config);
    }
}

QString QAmqpClient::gitVersion()
{
    return QString(GIT_VERSION);
}

void QAmqpClient::ignoreSslErrors(const QList<QSslError> &errors)
{
    Q_D(QAmqpClient);

    if (errors.isEmpty()) {
        qAmqpDebug() << Q_FUNC_INFO << "clearing ignored SSL errors";
    } else {
        foreach (const QSslError &sslError, errors) {
            qAmqpDebug() << Q_FUNC_INFO << "ignoring SSL error:" << sslError.errorString();
        }
    }

    d->socket->ignoreSslErrors(errors);
}

void QAmqpClient::connectToHost(const QString &uri)
{
    Q_D(QAmqpClient);
    if (uri.isEmpty()) {
        d->_q_connect();
        return;
    }

    if (!d->parseConnectionString(uri)) {
        d->error = QAMQP::SyntaxError;
        d->errorString = QLatin1String("Invalid AMQP URI: expected amqp:// or amqps:// with a host");
        Q_EMIT error(d->error);
        return;
    }
    d->_q_connect();
}

void QAmqpClient::connectToHost(const QHostAddress &address, quint16 port)
{
    Q_D(QAmqpClient);
    d->host = address.toString();
    d->port = port;
    d->_q_connect();
}

void QAmqpClient::disconnectFromHost()
{
    Q_D(QAmqpClient);
    d->_q_disconnect();
}

void QAmqpClient::abort()
{
    Q_D(QAmqpClient);
    d->closeConnection();
}

#include "moc_qamqpclient.cpp"
