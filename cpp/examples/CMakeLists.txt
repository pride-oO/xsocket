add_executable(xsocket_example_server server.cpp)
target_link_libraries(xsocket_example_server PRIVATE xsocket::xsocket)

add_executable(xsocket_example_client client.cpp)
target_link_libraries(xsocket_example_client PRIVATE xsocket::xsocket)
