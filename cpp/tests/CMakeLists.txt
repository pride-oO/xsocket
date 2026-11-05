add_executable(xsocket_tests tests.cpp)
target_link_libraries(xsocket_tests PRIVATE xsocket::xsocket)
if(NOT MSVC)
    target_compile_options(xsocket_tests PRIVATE -Wall -Wextra)
endif()

foreach(name
    message_codec
    event_emitter
    request_response
    error_response
    ttl_timeout
    binary
    auth_deny
    reconnect_session
    session_timeout
    client_close
    server_close
    listen_error
    wss
)
    add_test(NAME ${name} COMMAND xsocket_tests ${name} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set_tests_properties(${name} PROPERTIES TIMEOUT 60 RUN_SERIAL TRUE)
endforeach()
