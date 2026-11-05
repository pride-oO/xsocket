cmake_minimum_required(VERSION 3.16)

project(xsocket VERSION 2.4.0 LANGUAGES CXX)

set(XSOCKET_IS_MAIN_PROJECT OFF)
if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(XSOCKET_IS_MAIN_PROJECT ON)
endif()

option(XSOCKET_BUILD_TESTS "Build xsocket tests" ${XSOCKET_IS_MAIN_PROJECT})
option(XSOCKET_BUILD_EXAMPLES "Build xsocket examples" ${XSOCKET_IS_MAIN_PROJECT})
option(XSOCKET_FETCH_JSON "Download nlohmann_json if not found" ON)
set(XSOCKET_SANITIZE "" CACHE STRING "Sanitizers: address,undefined or thread")

if(APPLE AND NOT OPENSSL_ROOT_DIR AND NOT DEFINED ENV{OPENSSL_ROOT_DIR})
    foreach(prefix /opt/homebrew/opt/openssl@3 /usr/local/opt/openssl@3)
        if(EXISTS ${prefix})
            set(OPENSSL_ROOT_DIR ${prefix})
            break()
        endif()
    endforeach()
endif()

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost 1.81 CONFIG REQUIRED)

find_package(nlohmann_json 3.10 QUIET)
if(NOT nlohmann_json_FOUND)
    if(NOT XSOCKET_FETCH_JSON)
        message(FATAL_ERROR "nlohmann_json not found")
    endif()
    include(FetchContent)
    FetchContent_Declare(nlohmann_json
        URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
    )
    FetchContent_MakeAvailable(nlohmann_json)
endif()

add_library(xsocket
    src/client.cpp
    src/connection.cpp
    src/data.cpp
    src/event_emitter.cpp
    src/helpers.cpp
    src/server.cpp
    src/socket_object.cpp
)
add_library(xsocket::xsocket ALIAS xsocket)

target_compile_features(xsocket PUBLIC cxx_std_17)
target_include_directories(xsocket PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(xsocket PUBLIC
    OpenSSL::SSL
    OpenSSL::Crypto
    Boost::headers
    nlohmann_json::nlohmann_json
    Threads::Threads
)
target_compile_definitions(xsocket PRIVATE BOOST_ASIO_NO_DEPRECATED)

if(MSVC)
    target_compile_options(xsocket PRIVATE /W4 /bigobj)
else()
    target_compile_options(xsocket PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(XSOCKET_SANITIZE)
    target_compile_options(xsocket PUBLIC -fsanitize=${XSOCKET_SANITIZE} -fno-omit-frame-pointer -g)
    target_link_options(xsocket PUBLIC -fsanitize=${XSOCKET_SANITIZE})
endif()

if(XSOCKET_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(XSOCKET_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
