cmake_minimum_required(VERSION 3.14)

project(QAMQP VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_AUTOMOC ON)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

find_package(QT NAMES Qt6 Qt5 REQUIRED COMPONENTS Core Network)
find_package(Qt${QT_VERSION_MAJOR} REQUIRED COMPONENTS Core Network)

set(QAMQP_PUBLIC_HEADERS
    src/qamqpauthenticator.h
    src/qamqpchannel.h
    src/qamqpclient.h
    src/qamqpexchange.h
    src/qamqpglobal.h
    src/qamqpmessage.h
    src/qamqpqueue.h
    src/qamqptable.h
)

set(QAMQP_SOURCES
    src/qamqpauthenticator.cpp
    src/qamqpchannel.cpp
    src/qamqpchannelhash.cpp
    src/qamqpclient.cpp
    src/qamqpexchange.cpp
    src/qamqpframe.cpp
    src/qamqpmessage.cpp
    src/qamqpqueue.cpp
    src/qamqptable.cpp
)

add_library(qamqp ${QAMQP_SOURCES} ${QAMQP_PUBLIC_HEADERS})
add_library(QAMQP::qamqp ALIAS qamqp)

set_target_properties(qamqp PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    EXPORT_NAME qamqp
)

set(QAMQP_GIT_VERSION "")
execute_process(
    COMMAND git describe --always --dirty
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    OUTPUT_VARIABLE QAMQP_GIT_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE QAMQP_GIT_RESULT
)
if(NOT QAMQP_GIT_RESULT EQUAL 0 OR QAMQP_GIT_VERSION STREQUAL "")
    set(QAMQP_GIT_VERSION "${PROJECT_VERSION}")
endif()

target_compile_definitions(qamqp
    PRIVATE QAMQP_BUILD GIT_VERSION="${QAMQP_GIT_VERSION}"
    PUBLIC $<$<BOOL:${BUILD_SHARED_LIBS}>:QAMQP_SHARED>
)
target_include_directories(qamqp PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/qamqp>
)
target_link_libraries(qamqp PUBLIC Qt${QT_VERSION_MAJOR}::Core Qt${QT_VERSION_MAJOR}::Network)

install(TARGETS qamqp
    EXPORT QAMQPTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/qamqp
)
install(FILES ${QAMQP_PUBLIC_HEADERS}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/qamqp
)
install(FILES LICENSE README.md
    DESTINATION ${CMAKE_INSTALL_DATADIR}/doc/QAMQP
)
install(EXPORT QAMQPTargets
    FILE QAMQPTargets.cmake
    NAMESPACE QAMQP::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/QAMQP
)

configure_package_config_file(
    cmake/QAMQPConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/QAMQPConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/QAMQP
)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/QAMQPConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/QAMQPConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/QAMQPConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/QAMQP
)