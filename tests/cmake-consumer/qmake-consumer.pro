# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (C) 2026 Fabian Suhr (YearsLaterFixed)
QT += core network
QT -= gui
CONFIG += console
equals(QT_MAJOR_VERSION, 5): CONFIG += c++11
TEMPLATE = app
TARGET = qamqp-qmake-consumer
SOURCES += main.cpp
INCLUDEPATH += $${QAMQP_PREFIX}/include/qamqp
LIBS += -L$${QAMQP_PREFIX}/lib -lqamqp
QMAKE_RPATHDIR += $${QAMQP_PREFIX}/lib