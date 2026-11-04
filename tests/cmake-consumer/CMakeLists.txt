cmake_minimum_required(VERSION 3.14)

project(QAMQPCMakeConsumer LANGUAGES CXX)

find_package(QAMQP CONFIG REQUIRED)

add_executable(qamqp-consumer main.cpp)
target_link_libraries(qamqp-consumer PRIVATE QAMQP::qamqp)