#include <QDateTime>
/* Fork modifications Copyright (C) 2026 Fabian Suhr (YearsLaterFixed).
 * Modified: 2026-11-04. Upstream: https://github.com/mbroadst/qamqp
 * Fork/Contact: https://github.com/YearsLaterFixed/qamqp
 * See LICENSING.md at repository root for inherited rights and notices. */
#include <QDebug>
#include <QIODevice>
#include <QList>

#include "qamqptable.h"
#include "qamqpglobal.h"
#include "qamqpframe_p.h"

QReadWriteLock QAmqpFrame::lock_;
int QAmqpFrame::writeTimeout_ = 1000;

QAmqpFrame::QAmqpFrame(FrameType type)
    : size_(0),
      type_(type),
      channel_(0)
{
}

QAmqpFrame::FrameType QAmqpFrame::type() const
{
    return static_cast<QAmqpFrame::FrameType>(type_);
}

QAmqpFrame::~QAmqpFrame()
{
}

void QAmqpFrame::setChannel(quint16 channel)
{
    channel_ = channel;
}

int QAmqpFrame::writeTimeout()
{
    QReadLocker locker(&lock_);
    return writeTimeout_;
}

void QAmqpFrame::setWriteTimeout(int msecs)
{
    QWriteLocker locker(&lock_);
    writeTimeout_ = msecs;
}

quint16 QAmqpFrame::channel() const
{
    return channel_;
}

qint32 QAmqpFrame::size() const
{
    return 0;
}

/*
void QAmqpFrame::readEnd(QDataStream &stream)
{
    unsigned char end_  = 0;
    stream.readRawData(reinterpret_cast<char*>(&end_), sizeof(end_));
    if (end_ != FRAME_END)
        qAmqpDebug("Wrong end of frame");
}
*/

QDataStream &operator<<(QDataStream &stream, const QAmqpFrame &frame)
{
    const qint32 payloadSize = frame.size();
    if (payloadSize < 0) {
        stream.setStatus(QDataStream::WriteFailed);
        return stream;
    }

    // write header
    stream << frame.type_;
    stream << frame.channel_;
    stream << payloadSize;

    frame.writePayload(stream);
    if (stream.status() != QDataStream::Ok)
        return stream;

    // write end
    stream << qint8(QAmqpFrame::FRAME_END);
    
    // rely on Qt's async write buffering (QIODevice::bytesWritten()) instead of
    // blocking the event loop here; writeTimeout is kept only for API compatibility

    return stream;
}

QDataStream &operator>>(QDataStream &stream, QAmqpFrame &frame)
{
    stream >> frame.type_;
    stream >> frame.channel_;
    stream >> frame.size_;
    const qint64 payloadStart = stream.device()->pos();
    frame.readPayload(stream);
    if (stream.status() == QDataStream::Ok &&
        stream.device()->pos() - payloadStart != frame.size_)
        stream.setStatus(QDataStream::ReadCorruptData);
    return stream;
}

//////////////////////////////////////////////////////////////////////////

QAmqpMethodFrame::QAmqpMethodFrame()
    : QAmqpFrame(QAmqpFrame::Method),
      methodClass_(0),
    id_(0),
    argumentsValid_(true)
{
}

QAmqpMethodFrame::QAmqpMethodFrame(MethodClass methodClass, qint16 id)
    : QAmqpFrame(QAmqpFrame::Method),
      methodClass_(methodClass),
    id_(id),
    argumentsValid_(true)
{
}

QAmqpFrame::MethodClass QAmqpMethodFrame::methodClass() const
{
    return MethodClass(methodClass_);
}

qint16 QAmqpMethodFrame::id() const
{
    return id_;
}

qint32 QAmqpMethodFrame::size() const
{
    return argumentsValid_ ? sizeof(id_) + sizeof(methodClass_) + arguments_.size() : -1;
}

void QAmqpMethodFrame::setArguments(const QByteArray &data, QDataStream::Status status)
{
    arguments_ = data;
    argumentsValid_ = status == QDataStream::Ok;
}

QByteArray QAmqpMethodFrame::arguments() const
{
    return arguments_;
}

void QAmqpMethodFrame::readPayload(QDataStream &stream)
{
    stream >> methodClass_;
    stream >> id_;

    const qint32 headerSize = qint32(sizeof(id_) + sizeof(methodClass_));
    if (size_ < headerSize) {
        // declared frame size smaller than the mandatory methodClass_/id_ fields:
        // size_ - headerSize would underflow and resize() with a huge/negative value
        qAmqpDebug() << Q_FUNC_INFO << "method frame size too small:" << size_;
        stream.setStatus(QDataStream::ReadCorruptData);
        arguments_.clear();
        return;
    }

    arguments_.resize(size_ - headerSize);
    stream.readRawData(arguments_.data(), arguments_.size());
    argumentsValid_ = stream.status() == QDataStream::Ok;
}

void QAmqpMethodFrame::writePayload(QDataStream &stream) const
{
    if (!argumentsValid_) {
        stream.setStatus(QDataStream::WriteFailed);
        return;
    }
    stream << quint16(methodClass_);
    stream << quint16(id_);
    stream.writeRawData(arguments_.data(), arguments_.size());
}

//////////////////////////////////////////////////////////////////////////

bool QAmqpFrame::validateFieldSize(QDataStream &s, qint64 size)
{
    if (size < 0 || size > AMQP_FRAME_MAX) {
        qAmqpDebug() << Q_FUNC_INFO << "field size out of range:" << size;
        s.setStatus(QDataStream::ReadCorruptData);
        return false;
    }

    QIODevice *device = s.device();
    if (device && size > device->bytesAvailable()) {
        qAmqpDebug() << Q_FUNC_INFO << "field size exceeds available data:" << size;
        s.setStatus(QDataStream::ReadCorruptData);
        return false;
    }

    return true;
}

QVariant QAmqpFrame::readAmqpField(QDataStream &s, QAmqpMetaType::ValueType type)
{
    switch (type) {
    case QAmqpMetaType::Boolean:
    {
        quint8 octet = 0;
        s >> octet;
        return QVariant::fromValue<bool>(octet > 0);
    }
    case QAmqpMetaType::ShortShortUint:
    {
        quint8 octet = 0;
        s >> octet;
        return QVariant::fromValue<int>(octet);
    }
    case QAmqpMetaType::ShortUint:
    {
        quint16 tmp_value = 0;
        s >> tmp_value;
        return QVariant::fromValue<uint>(tmp_value);
    }
    case QAmqpMetaType::LongUint:
    {
        quint32 tmp_value = 0;
        s >> tmp_value;
        return QVariant::fromValue<uint>(tmp_value);
    }
    case QAmqpMetaType::LongLongUint:
    {
        qulonglong v = 0 ;
        s >> v;
        return v;
    }
    case QAmqpMetaType::ShortString:
    {
        quint8 size = 0;
        QByteArray buffer;

        s >> size;
        if (!validateFieldSize(s, size))
            return QVariant();
        buffer.resize(size);
        s.readRawData(buffer.data(), buffer.size());
        return QString::fromUtf8(buffer.data(), buffer.size());
    }
    case QAmqpMetaType::LongString:
    {
        quint32 size = 0;
        QByteArray buffer;

        s >> size;
        if (!validateFieldSize(s, size))
            return QVariant();
        buffer.resize(size);
        s.readRawData(buffer.data(), buffer.size());
        return QString::fromUtf8(buffer.data(), buffer.size());
    }
    case QAmqpMetaType::Timestamp:
    {
        qulonglong tmp_value;
        s >> tmp_value;
#if (QT_VERSION >= QT_VERSION_CHECK(5, 8, 0))
        return QDateTime::fromSecsSinceEpoch(tmp_value);
#else
        return QDateTime::fromTime_t(tmp_value);
#endif
    }
    case QAmqpMetaType::Hash:
    {
        QAmqpTable table;
        s >> table;
        return table;
    }
    case QAmqpMetaType::Void:
        return QVariant();
    default:
        qAmqpDebug() << Q_FUNC_INFO << "unsupported value type: " << type;
    }

    return QVariant();
}

bool QAmqpFrame::isValidShortString(const QString &value)
{
    return value.toUtf8().size() <= 255;
}

bool QAmqpFrame::writeAmqpField(QDataStream &s, QAmqpMetaType::ValueType type,
                                 const QVariant &value)
{
    if (s.status() != QDataStream::Ok)
        return false;

    switch (type) {
    case QAmqpMetaType::Boolean:
        s << (value.toBool() ? qint8(1) : qint8(0));
        break;
    case QAmqpMetaType::ShortShortUint:
        s << qint8(value.toUInt());
        break;
    case QAmqpMetaType::ShortUint:
        s << quint16(value.toUInt());
        break;
    case QAmqpMetaType::LongUint:
        s << quint32(value.toUInt());
        break;
    case QAmqpMetaType::LongLongUint:
        s << qulonglong(value.toULongLong());
        break;
    case QAmqpMetaType::ShortString:
    {
        QByteArray bytes = value.toString().toUtf8();
        if (bytes.length() > 255) {
            qAmqpDebug() << Q_FUNC_INFO << "shortstr exceeds 255 bytes: " << bytes.length();
            s.setStatus(QDataStream::WriteFailed);
            return false;
        }

        s << quint8(bytes.length());
        s.writeRawData(bytes.constData(), bytes.length());
    }
        break;
    case QAmqpMetaType::LongString:
    {
        QByteArray bytes = value.toString().toUtf8();
        s << quint32(bytes.length());
        s.writeRawData(bytes.constData(), bytes.length());
    }
        break;
    case QAmqpMetaType::Timestamp:
#if (QT_VERSION >= QT_VERSION_CHECK(5, 8, 0))
        s << qulonglong(value.toDateTime().toSecsSinceEpoch());
#else
        s << qulonglong(value.toDateTime().toTime_t());
#endif
        break;
    case QAmqpMetaType::Hash:
    {
        QAmqpTable table(value.toHash());
        s << table;
    }
        break;
    default:
        qAmqpDebug() << Q_FUNC_INFO << "unsupported value type: " << type;
        s.setStatus(QDataStream::WriteFailed);
        return false;
    }

    return s.status() == QDataStream::Ok;
}

//////////////////////////////////////////////////////////////////////////

QAmqpContentFrame::QAmqpContentFrame()
        : QAmqpFrame(QAmqpFrame::Header),
            serializationValid_(true)
{
}

QAmqpContentFrame::QAmqpContentFrame(QAmqpFrame::MethodClass methodClass)
        : QAmqpFrame(QAmqpFrame::Header),
            serializationValid_(true)
{
    methodClass_ = methodClass;
}

QAmqpFrame::MethodClass QAmqpContentFrame::methodClass() const
{
    return MethodClass(methodClass_);
}

qint32 QAmqpContentFrame::size() const
{
    serializationValid_ = false;
    QDataStream out(&buffer_, QIODevice::WriteOnly);
    buffer_.clear();
    out << qint16(methodClass_);
    out << qint16(0); //weight
    out << qlonglong(bodySize_);

    qint16 prop_ = 0;
    foreach (int p, properties_.keys())
        prop_ |= p;
    out << prop_;

    if (prop_ & QAmqpMessage::ContentType)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::ContentType]);

    if (prop_ & QAmqpMessage::ContentEncoding)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::ContentEncoding]);

    if (prop_ & QAmqpMessage::Headers)
        writeAmqpField(out, QAmqpMetaType::Hash, properties_[QAmqpMessage::Headers]);

    if (prop_ & QAmqpMessage::DeliveryMode)
        writeAmqpField(out, QAmqpMetaType::ShortShortUint, properties_[QAmqpMessage::DeliveryMode]);

    if (prop_ & QAmqpMessage::Priority)
        writeAmqpField(out, QAmqpMetaType::ShortShortUint, properties_[QAmqpMessage::Priority]);

    if (prop_ & QAmqpMessage::CorrelationId)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::CorrelationId]);

    if (prop_ & QAmqpMessage::ReplyTo)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::ReplyTo]);

    if (prop_ & QAmqpMessage::Expiration)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::Expiration]);

    if (prop_ & QAmqpMessage::MessageId)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::MessageId]);

    if (prop_ & QAmqpMessage::Timestamp)
        writeAmqpField(out, QAmqpMetaType::Timestamp, properties_[QAmqpMessage::Timestamp]);

    if (prop_ & QAmqpMessage::Type)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::Type]);

    if (prop_ & QAmqpMessage::UserId)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::UserId]);

    if (prop_ & QAmqpMessage::AppId)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::AppId]);

    if (prop_ & QAmqpMessage::ClusterID)
        writeAmqpField(out, QAmqpMetaType::ShortString, properties_[QAmqpMessage::ClusterID]);

    if (out.status() != QDataStream::Ok)
        return -1;

    serializationValid_ = true;

    return buffer_.size();
}

qlonglong QAmqpContentFrame::bodySize() const
{
    return bodySize_;
}

void QAmqpContentFrame::setBodySize(qlonglong size)
{
    bodySize_ = size;
}

void QAmqpContentFrame::setProperty(QAmqpMessage::Property prop, const QVariant &value)
{
    properties_[prop] = value;
}

QVariant QAmqpContentFrame::property(QAmqpMessage::Property prop) const
{
    return properties_.value(prop);
}

void QAmqpContentFrame::writePayload(QDataStream &out) const
{
    out.writeRawData(buffer_.data(), buffer_.size());
}

void QAmqpContentFrame::readPayload(QDataStream &in)
{
    in >> methodClass_;
    qint16 weight = 0;
    in >> weight;
    in >> bodySize_;
    quint16 flags_ = 0;
    in >> flags_;
    if (in.status() != QDataStream::Ok || weight != 0 || bodySize_ < 0 ||
        (flags_ & quint16(0x0003)) != 0) {
        in.setStatus(QDataStream::ReadCorruptData);
        return;
    }

    if (flags_ & QAmqpMessage::ContentType)
        properties_[QAmqpMessage::ContentType] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::ContentEncoding)
        properties_[QAmqpMessage::ContentEncoding] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::Headers)
        properties_[QAmqpMessage::Headers] = readAmqpField(in, QAmqpMetaType::Hash);

    if (flags_ & QAmqpMessage::DeliveryMode)
        properties_[QAmqpMessage::DeliveryMode] = readAmqpField(in, QAmqpMetaType::ShortShortUint);

    if (flags_ & QAmqpMessage::Priority)
        properties_[QAmqpMessage::Priority] = readAmqpField(in, QAmqpMetaType::ShortShortUint);

    if (flags_ & QAmqpMessage::CorrelationId)
        properties_[QAmqpMessage::CorrelationId] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::ReplyTo)
        properties_[QAmqpMessage::ReplyTo] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::Expiration)
        properties_[QAmqpMessage::Expiration] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::MessageId)
        properties_[QAmqpMessage::MessageId] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::Timestamp)
        properties_[QAmqpMessage::Timestamp] = readAmqpField(in, QAmqpMetaType::Timestamp);

    if (flags_ & QAmqpMessage::Type)
        properties_[QAmqpMessage::Type] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::UserId)
        properties_[QAmqpMessage::UserId] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::AppId)
        properties_[QAmqpMessage::AppId] = readAmqpField(in, QAmqpMetaType::ShortString);

    if (flags_ & QAmqpMessage::ClusterID)
        properties_[QAmqpMessage::ClusterID] = readAmqpField(in, QAmqpMetaType::ShortString);
}

//////////////////////////////////////////////////////////////////////////

QAmqpContentBodyFrame::QAmqpContentBodyFrame()
    : QAmqpFrame(QAmqpFrame::Body)
{
}

void QAmqpContentBodyFrame::setBody(const QByteArray &data)
{
    body_ = data;
}

QByteArray QAmqpContentBodyFrame::body() const
{
    return body_;
}

void QAmqpContentBodyFrame::writePayload(QDataStream &out) const
{
    out.writeRawData(body_.data(), body_.size());
}

void QAmqpContentBodyFrame::readPayload(QDataStream &in)
{
    body_.resize(size_);
    in.readRawData(body_.data(), body_.size());
}

qint32 QAmqpContentBodyFrame::size() const
{
    return body_.size();
}

//////////////////////////////////////////////////////////////////////////

QAmqpHeartbeatFrame::QAmqpHeartbeatFrame()
    : QAmqpFrame(QAmqpFrame::Heartbeat)
{
}

void QAmqpHeartbeatFrame::readPayload(QDataStream &stream)
{
    Q_UNUSED(stream)
}

void QAmqpHeartbeatFrame::writePayload(QDataStream &stream) const
{
    Q_UNUSED(stream)
}
