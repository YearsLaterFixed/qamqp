# Fork modifications Copyright (C) 2026 Fabian Suhr (YearsLaterFixed).
# Modified: 2026-11-04. See LICENSING.md for inherited rights and notices.
# Fork/Contact: https://github.com/YearsLaterFixed/qamqp
include(../qamqp.pri)

INCLUDEPATH += .
TEMPLATE = lib
TARGET = qamqp
build_pass:CONFIG(debug, debug|release) {
    TARGET = $$join(TARGET,,,d)
}
QT += core network
QT -= gui
DEFINES += QAMQP_BUILD
CONFIG += $${QAMQP_LIBRARY_TYPE}
CONFIG += nostrip
VERSION = $${QAMQP_VERSION}
win32:DESTDIR = $$OUT_PWD
macx:QMAKE_LFLAGS_SONAME = -Wl,-install_name,@rpath/

# for some reason with Travis' qt 5.0.2 you can't chain these with an |
NEED_GCOV_SUPPORT = 0
greaterThan(QT_MAJOR_VERSION, 4):lessThan(QT_MINOR_VERSION, 2) {
    NEED_GCOV_SUPPORT = 1
}
lessThan(QT_MAJOR_VERSION, 5):lessThan(QT_MINOR_VERSION, 9):lessThan(QT_PATCH_VERSION, 6) {
    NEED_GCOV_SUPPORT = 1
}

greaterThan(NEED_GCOV_SUPPORT, 0) {
    # NOTE: remove when travis adds a newer ubuntu, or when hell freezes over
    gcov {
        QMAKE_CFLAGS           += -fprofile-arcs -ftest-coverage
        QMAKE_CXXFLAGS         += -fprofile-arcs -ftest-coverage
        QMAKE_OBJECTIVE_CFLAGS += -fprofile-arcs -ftest-coverage
        QMAKE_LFLAGS           += -fprofile-arcs -ftest-coverage
        QMAKE_CLEAN += $(OBJECTS_DIR)*.gcno and $(OBJECTS_DIR)*.gcda
    }
}

GIT_VERSION = $${QAMQP_VERSION}
exists($$PWD/../.git) {
    GIT_DESCRIPTION = $$system(git -C $$shell_quote($$PWD/..) describe --always --dirty --tags)
    !isEmpty(GIT_DESCRIPTION): GIT_VERSION = $$GIT_DESCRIPTION
}
DEFINES += GIT_VERSION=\\\"$$GIT_VERSION\\\"

PRIVATE_HEADERS += \
    qamqpchannel_p.h \
    qamqpchannelhash_p.h \
    qamqpclient_p.h \
    qamqpexchange_p.h \
    qamqpframe_p.h \
    qamqpmessage_p.h \
    qamqpqueue_p.h

INSTALL_HEADERS += \
    qamqpauthenticator.h \
    qamqpchannel.h \
    qamqpclient.h \
    qamqpexchange.h \
    qamqpglobal.h \
    qamqpmessage.h \
    qamqpqueue.h \
    qamqptable.h

HEADERS += \
    $${INSTALL_HEADERS} \
    $${PRIVATE_HEADERS}

SOURCES += \
    qamqpauthenticator.cpp \
    qamqpchannel.cpp \
    qamqpchannelhash.cpp \
    qamqpclient.cpp \
    qamqpexchange.cpp \
    qamqpframe.cpp \
    qamqpmessage.cpp \
    qamqpqueue.cpp \
    qamqptable.cpp

DISTFILES += \
    ../LICENSE \
    ../COPYING.LESSER \
    ../LICENSING.md \
    ../LICENSES/MIT-historical.txt \
    ../CHANGELOG.md \
    ../README.md

# install
headers.files = $${INSTALL_HEADERS}
headers.path = $${PREFIX}/include/qamqp
target.path = $${PREFIX}/$${LIBDIR}
INSTALLS += headers target
license_docs.files = ../LICENSE ../COPYING.LESSER ../LICENSING.md ../CHANGELOG.md ../README.md
license_docs.path = $${PREFIX}/share/doc/QAMQP
historical_license.files = ../LICENSES/MIT-historical.txt
historical_license.path = $${PREFIX}/share/doc/QAMQP/LICENSES
INSTALLS += license_docs historical_license
release_docs.files = ../docs/RELEASE.md ../docs/VALIDATION.md
release_docs.path = $${PREFIX}/share/doc/QAMQP/docs
INSTALLS += release_docs

# pkg-config support
CONFIG += create_pc create_prl no_install_prl
QMAKE_PKGCONFIG_DESTDIR = pkgconfig
QMAKE_PKGCONFIG_LIBDIR = $$target.path
QMAKE_PKGCONFIG_INCDIR = $$headers.path
equals(QAMQP_LIBRARY_TYPE, staticlib) {
    QMAKE_PKGCONFIG_CFLAGS = -DQAMQP_STATIC
} else {
    QMAKE_PKGCONFIG_CFLAGS = -DQAMQP_SHARED
}
unix:QMAKE_CLEAN += -r pkgconfig lib$${TARGET}.prl
