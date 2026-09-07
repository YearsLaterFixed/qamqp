#include <QtTest/QtTest>

#include "qamqptable.h"

class tst_QAMQPParser : public QObject
{
    Q_OBJECT
private Q_SLOTS:
    void rejectsExcessivelyNestedArray();
    void rejectsExcessivelyNestedTable();
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

void tst_QAMQPParser::rejectsExcessivelyNestedTable()
{
    const int depth = 33;
    QByteArray nestedTable;
    QDataStream emptyTable(&nestedTable, QIODevice::WriteOnly);
    emptyTable << quint32(0);

    for (int i = 1; i < depth; ++i) {
        QByteArray tableBody;
        QDataStream tableBodyStream(&tableBody, QIODevice::WriteOnly);
        tableBodyStream << quint8(1);
        tableBodyStream.writeRawData("n", 1);
        tableBodyStream << qint8('F');
        tableBodyStream.writeRawData(nestedTable.constData(), nestedTable.size());

        QByteArray wrappedTable;
        QDataStream wrappedStream(&wrappedTable, QIODevice::WriteOnly);
        wrappedStream << quint32(tableBody.size());
        wrappedStream.writeRawData(tableBody.constData(), tableBody.size());
        nestedTable = wrappedTable;
    }

    QDataStream input(&nestedTable, QIODevice::ReadOnly);
    QAmqpTable table;
    input >> table;

    QCOMPARE(input.status(), QDataStream::ReadCorruptData);
}

QTEST_APPLESS_MAIN(tst_QAMQPParser)
#include "tst_qamqpparser.moc"