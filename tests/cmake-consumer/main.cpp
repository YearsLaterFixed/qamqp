#include <qamqpclient.h>
/* Copyright (C) 2026 Fabian Suhr (YearsLaterFixed).
 * Modified: 2026-11-04. SPDX-License-Identifier: LGPL-2.1-or-later */
#include <QCoreApplication>

int main(int argc, char **argv)
{
    QCoreApplication application(argc, argv);
    QAmqpClient client;
    return client.isConnected() || QString::fromLatin1(QAMQP_VERSION) != QLatin1String("1.0.0") ||
        QAmqpClient::gitVersion() != QLatin1String("1.0.0") ? 1 : 0;
}