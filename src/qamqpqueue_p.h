#ifndef QAMQPQUEUE_P_H
/* Fork modifications Copyright (C) 2026 Fabian Suhr (YearsLaterFixed).
 * Modified: 2026-11-04. Upstream: https://github.com/mbroadst/qamqp
 * Fork/Contact: https://github.com/YearsLaterFixed/qamqp
 * See LICENSING.md at repository root for inherited rights and notices. */
#define QAMQPQUEUE_P_H

#include <QQueue>
#include <QStringList>

#include "qamqpchannel_p.h"

class QAMQP_EXPORT QAmqpQueuePrivate: public QAmqpChannelPrivate,
                         public QAmqpContentFrameHandler,
                         public QAmqpContentBodyFrameHandler
{
public:
    struct DelayedBinding {
        QString exchangeName;
        QString routingKey;
        bool noWait;
        QAmqpTable arguments;
    };

    enum MethodId {
        METHOD_ID_ENUM(miDeclare, 10),
        METHOD_ID_ENUM(miBind, 20),
        METHOD_ID_ENUM(miUnbind, 50),
        METHOD_ID_ENUM(miPurge, 30),
        METHOD_ID_ENUM(miDelete, 40)
    };

    QAmqpQueuePrivate(QAmqpQueue *q);
    ~QAmqpQueuePrivate();

    static bool isAcceptableBodySize(qlonglong declaredBodySize);
    static bool isAcceptableBodyChunk(qlonglong remainingSize, qlonglong chunkSize);
    static QAmqpQueue::OverflowReason overflowReason(int bufferedCount,
                                                      qlonglong bufferedBytes,
                                                      qlonglong declaredBodySize,
                                                      int maxBufferedMessages,
                                                      qlonglong maxBufferedBytes,
                                                      qlonglong maxMessageSize);
    void enqueueMessage(const QAmqpMessage &message);
    QAmqpMessage dequeueMessage();
    void clearMessages();

    virtual void resetInternalState();

    void declare();
    virtual bool _q_method(const QAmqpMethodFrame &frame);

    // AMQP Queue method handlers
    void declareOk(const QAmqpMethodFrame &frame);
    void deleteOk(const QAmqpMethodFrame &frame);
    void purgeOk(const QAmqpMethodFrame &frame);
    void bindOk(const QAmqpMethodFrame &frame);
    void unbindOk(const QAmqpMethodFrame &frame);
    void consumeOk(const QAmqpMethodFrame &frame);

    // AMQP Basic method handlers
    virtual void _q_content(const QAmqpContentFrame &frame);
    virtual void _q_body(const QAmqpContentBodyFrame &frame);
    void deliver(const QAmqpMethodFrame &frame);
    void getOk(const QAmqpMethodFrame &frame);
    void cancelOk(const QAmqpMethodFrame &frame);

    QString type;
    int options;
    bool delayedDeclare;
    bool declared;
    QQueue<DelayedBinding> delayedBindings;

    QString consumerTag;
    bool recievingMessage;
    QAmqpMessage currentMessage;
    bool consuming;
    bool consumeRequested;

    qint32 messageCount;
    qint32 consumerCount;
    QQueue<QAmqpMessage> messages;
    qlonglong bufferedBytes;
    int maxBufferedMessages;
    qlonglong maxBufferedBytes;
    qlonglong maxMessageSize;
    bool discardingMessage;
    QAmqpQueue::OverflowReason discardReason;
    qlonglong discardedDeliveryTag;

    Q_DECLARE_PUBLIC(QAmqpQueue)

};

#endif // QAMQPQUEUE_P_H
