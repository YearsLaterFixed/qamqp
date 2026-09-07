#include <QtTest/QtTest>

#include "qamqptable.h"

class tst_QAMQPParser : public QObject
{
    Q_OBJECT
private Q_SLOTS:
    void rejectsExcessivelyNestedArray();
};

void tst_QAMQPParser::rejectsExcessivelyNestedArray()
{
    const int depth = 33;
    QByteArray nestedValue(1, 'V');

    for (int i = 1; i < depth; ++i) {
        QByteArray wrappedValue;
        QDataStream wrappedStream(&wrappedValue, QIODevice::WriteOnly);
        wrappedStream << qint8('A');
        wrappedStream << quint32(nestedValue.size());
        wrappedStream.writeRawData(nestedValue.constData(), nestedValue.size());
        nestedValue = wrappedValue;
    }

    QByteArray encodedArray;
    QDataStream output(&encodedArray, QIODevice::WriteOnly);
    output << quint32(nestedValue.size());
    output.writeRawData(nestedValue.constData(), nestedValue.size());

    QDataStream input(&encodedArray, QIODevice::ReadOnly);
    QVariant value = QAmqpTable::readFieldValue(input, QAmqpMetaType::Array);

    QVERIFY(!value.isValid());
    QCOMPARE(input.status(), QDataStream::ReadCorruptData);
}

QTEST_APPLESS_MAIN(tst_QAMQPParser)
#include "tst_qamqpparser.moc"