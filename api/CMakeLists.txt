cmake_minimum_required(VERSION 3.16)
project(drivex_api CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Drogon CONFIG REQUIRED)

file(GLOB_RECURSE SOURCES
    src/*.cpp
)

add_executable(${PROJECT_NAME} ${SOURCES})
target_link_libraries(${PROJECT_NAME} PRIVATE Drogon::Drogon)

enable_testing()
add_subdirectory(tests)
