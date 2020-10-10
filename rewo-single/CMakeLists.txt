cmake_minimum_required(VERSION 3.13)
project(rewo-single)

set(CMAKE_CXX_STANDARD 11)

add_executable(rewo-single main.cpp api.cpp api.h exp.cpp exp.h factory.cpp factory.h hash.cpp hash.h storage.cpp storage.h trxn.h config.h cli.h cli.cpp)