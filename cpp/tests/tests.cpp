#include "harness.hpp"

#include "../src/connection.hpp"

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <atomic>
#include <cstdio>
#include <fstream>

using namespace xsocket;
using test::Loop;
using test::Signal;
using test::waitFuture;

namespace {

std::string url(int port, const std::string& scheme = "ws") {
    return scheme + "://localhost:" + std::to_string(port);
}

std::shared_ptr<Server> createEchoServer(Loop& loop, int port, json settings = json::object()) {
    settings["port"] = port;
    settings["host"] = "127.0.0.1";
    auto server = Server::create(loop.get(), settings);
    server->on("connect", [](std::shared_ptr<SocketObject> socketObject) {
        socketObject->on("data", [socketObject](std::shared_ptr<Data> socketData) {
            if (socketData->getType() == "silent") {
                return;
            }
            if (socketData->getType() == "deny") {
                socketData->response({}, "Permision denied");
                return;
            }
            if (socketData->getData().isBinary()) {
                Binary binary = socketData->getData().getBinary();
                for (auto& byte : binary) {
                    byte = static_cast<std::uint8_t>(byte + 1);
                }
                socketData->response(binary);
                return;
            }
            socketData->response({
                {"welcome", "Hello " + socketObject->getQueryVal("name", "unknown").get<std::string>()},
                {"echo", socketData->getData().getJSON()}
            });
        });
    });
    return server;
}

struct Certificate {
    std::string certPath;
    std::string keyPath;
};

Certificate createCertificate(const std::string& name, const std::string& commonName) {
    EVP_PKEY* key = EVP_RSA_gen(2048);
    X509* x509 = X509_new();
    ASN1_INTEGER_set(X509_get_serialNumber(x509), static_cast<long>(std::time(nullptr)));
    X509_gmtime_adj(X509_getm_notBefore(x509), -60);
    X509_gmtime_adj(X509_getm_notAfter(x509), 3600);
    X509_set_pubkey(x509, key);
    X509_NAME* subject = X509_get_subject_name(x509);
    X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char*>(commonName.c_str()), -1, -1, 0);
    X509_set_issuer_name(x509, subject);
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, x509, x509, nullptr, nullptr, 0);
    std::string san = "DNS:" + commonName + ",IP:127.0.0.1";
    if (X509_EXTENSION* extension = X509V3_EXT_conf_nid(nullptr, &ctx, NID_subject_alt_name, san.c_str())) {
        X509_add_ext(x509, extension, -1);
        X509_EXTENSION_free(extension);
    }
    if (X509_EXTENSION* extension = X509V3_EXT_conf_nid(nullptr, &ctx, NID_basic_constraints, "critical,CA:TRUE")) {
        X509_add_ext(x509, extension, -1);
        X509_EXTENSION_free(extension);
    }
    X509_sign(x509, key, EVP_sha256());
    Certificate certificate{"xsocket_test_" + name + "_cert.pem", "xsocket_test_" + name + "_key.pem"};
    FILE* certFile = std::fopen(certificate.certPath.c_str(), "wb");
    PEM_write_X509(certFile, x509);
    std::fclose(certFile);
    FILE* keyFile = std::fopen(certificate.keyPath.c_str(), "wb");
    PEM_write_PrivateKey(keyFile, key, nullptr, nullptr, 0, nullptr, nullptr);
    std::fclose(keyFile);
    X509_free(x509);
    EVP_PKEY_free(key);
    return certificate;
}

}

TEST_CASE(message_codec) {
    bool isText = false;
    std::string payload;
    CHECK(detail::encodeMessage(json::array({MSG_SD_CREATE, 1, "hello", 3000, nullptr, 123}), Value({{"a", 1}}), isText, payload));
    CHECK(isText);
    CHECK_EQ(payload, std::string(R"([1,1,"hello",3000,{"a":1},123])"));

    CHECK(detail::encodeMessage(json::array({MSG_SD_UPDATE, 7, nullptr, false}), Value(Binary{1, 2, 3}), isText, payload));
    CHECK(!isText);
    std::string header = R"([2,7,null,false])";
    CHECK_EQ(payload.size(), 4 + header.size() + 3);
    CHECK_EQ(static_cast<int>(payload[3]), static_cast<int>(header.size()));
    CHECK_EQ(payload.substr(4, header.size()), header);

    auto message = detail::decodeMessage(false, payload);
    CHECK(message.has_value());
    CHECK_EQ(message->body[1].get<int>(), 7);
    CHECK(message->binary);
    CHECK((*message->binary == Binary{1, 2, 3}));
    Value data = detail::getMessageData(*message, 2);
    CHECK(data.isBinary());

    CHECK(!detail::decodeMessage(true, "ping").has_value());
    CHECK(!detail::decodeMessage(true, "{}").has_value());
    CHECK(!detail::decodeMessage(false, std::string("\0\0\0\xff", 4)).has_value());

    json query = detail::parseQuery("name=Bob+Smith&x=%D0%9F&empty=&flag");
    CHECK_EQ(query["name"].get<std::string>(), std::string("Bob Smith"));
    CHECK_EQ(query["x"].get<std::string>(), std::string("\xD0\x9F"));
    CHECK_EQ(query["empty"].get<std::string>(), std::string(""));
    CHECK_EQ(query["flag"].get<std::string>(), std::string(""));
    CHECK_EQ(detail::encodeQuery({{"a b", "c/d"}, {"n", 1}}), std::string("a%20b=c%2Fd&n=1"));

    auto parsedUrl = detail::parseUrl("wss://[::1]:9300/path?x=1");
    CHECK(parsedUrl.has_value());
    CHECK(parsedUrl->isSecure);
    CHECK_EQ(parsedUrl->host, std::string("::1"));
    CHECK_EQ(parsedUrl->port, std::string("9300"));
    CHECK_EQ(parsedUrl->target, std::string("/path?x=1"));
    parsedUrl = detail::parseUrl("ws://example.com?xV=1");
    CHECK(parsedUrl.has_value());
    CHECK_EQ(parsedUrl->port, std::string("80"));
    CHECK_EQ(parsedUrl->target, std::string("/?xV=1"));
}

TEST_CASE(event_emitter) {
    EventEmitter emitter;
    int count = 0;
    std::string lastMsg;
    auto listenerID = emitter.on("test", [&](int value, const std::string& msg) {
        count += value;
        lastMsg = msg;
    });
    emitter.once("test", [&] {
        count += 100;
    });
    emitter.emit("test", 1, std::string("a"));
    emitter.emit("test", 2, std::string("b"));
    CHECK_EQ(count, 103);
    CHECK_EQ(lastMsg, std::string("b"));
    CHECK(emitter.off("test", listenerID));
    emitter.emit("test", 5, std::string("c"));
    CHECK_EQ(count, 103);
    CHECK_EQ(emitter.listenerCount("test"), std::size_t(0));
    emitter.on("error", [](const std::string&) {
        throw std::runtime_error("listener");
    });
    emitter.emit("error", std::string("x"));
}

TEST_CASE(request_response) {
    Loop loop;
    auto server = createEchoServer(loop, 9310);
    Signal<std::string> connectSignal;
    server->on("connect", [&](std::shared_ptr<SocketObject> socketObject) {
        connectSignal.set(socketObject->getQueryVal("name").get<std::string>() + "|" + socketObject->getIP());
    });
    waitFuture(server->listen());
    CHECK(server->isListen());
    CHECK_EQ(test::getErrorOf([&] { waitFuture(server->listen()); }), std::string("repeat"));

    auto client = Client::create(loop.get(), {url(9310)}, {{"name", "Bob"}});
    auto socketData = waitFuture(client->sendReady("hello", {{"message", "Hello server"}}, 2000));
    CHECK_EQ(socketData->getType(), std::string("hello"));
    CHECK_EQ(socketData->getID(), std::string("1"));
    CHECK_EQ(socketData->getResDataVal("welcome").get<std::string>(), std::string("Hello Bob"));
    CHECK_EQ(socketData->getResData().getJSON()["echo"]["message"].get<std::string>(), std::string("Hello server"));
    CHECK_EQ(connectSignal.wait(), std::string("Bob|127.0.0.1"));

    Signal<std::string> serverDataSignal;
    auto socketObject = client->getSocketObject();
    CHECK(socketObject);
    socketObject->on("data|fromServer", [&](std::shared_ptr<Data> socketData) {
        socketData->response({{"ok", true}});
    });
    auto serverSocketObject = server->getSocketObject(socketObject->getID());
    CHECK(serverSocketObject);
    serverSocketObject->sendReady("fromServer", {{"x", 1}}, 2000, [&](std::shared_ptr<Data> socketData, std::string error) {
        serverDataSignal.set(error.empty() ? socketData->getResData().getJSON().dump() : error);
    });
    CHECK_EQ(serverDataSignal.wait(), std::string(R"({"ok":true})"));

    auto sentData = waitFuture(client->send("log", {{"message", "Hello"}}, 0));
    CHECK_EQ(sentData->getTTL(), std::int64_t(0));

    waitFuture(client->close());
    waitFuture(server->close());
}

TEST_CASE(error_response) {
    Loop loop;
    auto server = createEchoServer(loop, 9311);
    waitFuture(server->listen());
    auto client = Client::create(loop.get(), {url(9311)});
    CHECK_EQ(test::getErrorOf([&] { waitFuture(client->sendReady("deny", {})); }), std::string("Permision denied"));
    waitFuture(client->close());
    waitFuture(server->close());
}

TEST_CASE(ttl_timeout) {
    Loop loop;
    auto server = createEchoServer(loop, 9312);
    Signal<std::shared_ptr<Data>> serverDataSignal;
    server->on("connect", [&](std::shared_ptr<SocketObject> socketObject) {
        socketObject->on("data|silent", [&](std::shared_ptr<Data> socketData) {
            serverDataSignal.set(socketData);
        });
    });
    waitFuture(server->listen());
    auto client = Client::create(loop.get(), {url(9312)});
    auto start = std::chrono::steady_clock::now();
    CHECK_EQ(test::getErrorOf([&] { waitFuture(client->sendReady("silent", {}, 300)); }), std::string("ttl"));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    CHECK(elapsed >= 250 && elapsed < 2000);
    auto serverData = serverDataSignal.wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    CHECK_EQ(serverData->getDestroy(), std::string("ttl"));
    CHECK_EQ(test::getErrorOf([&] { waitFuture(serverData->response({{"late", true}})); }), std::string("destroy"));
    waitFuture(client->close());
    waitFuture(server->close());
}

TEST_CASE(binary) {
    Loop loop;
    auto server = createEchoServer(loop, 9313);
    waitFuture(server->listen());
    auto client = Client::create(loop.get(), {url(9313)});
    client->on("data", [](std::shared_ptr<Data> socketData) {
        Binary binary = socketData->getData().getBinary();
        binary.push_back(0xFF);
        socketData->response(binary);
    });
    auto socketData = waitFuture(client->sendReady("file", Binary{1, 2, 3}));
    CHECK(socketData->getResData().isBinary());
    CHECK((socketData->getResData().getBinary() == Binary{2, 3, 4}));

    Binary bigBinary(5 * 1024 * 1024, 7);
    socketData = waitFuture(client->sendReady("file", bigBinary, 10000), 15000);
    CHECK_EQ(socketData->getResData().getBinary().size(), bigBinary.size());
    CHECK_EQ(static_cast<int>(socketData->getResData().getBinary()[100]), 8);

    auto socketObject = client->getSocketObject();
    auto serverSocketObject = server->getSocketObject(socketObject->getID());
    auto serverData = waitFuture(serverSocketObject->sendReady("toClient", Binary{9, 8}));
    CHECK((serverData->getResData().getBinary() == Binary{9, 8, 0xFF}));
    waitFuture(client->close());
    waitFuture(server->close());
}

TEST_CASE(auth_deny) {
    Loop loop;
    auto server = createEchoServer(loop, 9314);
    std::atomic<int> connectCount{0};
    server->use("auth", [](std::shared_ptr<SocketObject> socketObject, Server::SuccessCallback success, Server::FailedCallback failed) {
        bool isAllowed = helpers::isTruthy(socketObject->getQueryVal("name"));
        std::thread([isAllowed, success, failed] {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            if (isAllowed) {
                return success();
            }
            failed("Access denied");
        }).detach();
    });
    Signal<bool> connectSignal;
    server->on("connect", [&](std::shared_ptr<SocketObject>) {
        connectCount++;
        connectSignal.set(true);
    });
    waitFuture(server->listen());

    auto deniedClient = Client::create(loop.get(), {url(9314)});
    Signal<std::string> disconnectSignal;
    deniedClient->on("disconnect", [&](std::shared_ptr<SocketObject>, std::string msg) {
        disconnectSignal.set(msg);
    });
    CHECK_EQ(disconnectSignal.wait(), std::string("Access denied"));
    CHECK_EQ(connectCount.load(), 0);
    waitFuture(deniedClient->close());

    auto client = Client::create(loop.get(), {url(9314)}, {{"name", "Bob"}});
    CHECK(connectSignal.wait());
    auto socketData = waitFuture(client->sendReady("hello", {}));
    CHECK_EQ(socketData->getResDataVal("welcome").get<std::string>(), std::string("Hello Bob"));
    CHECK_EQ(connectCount.load(), 1);
    waitFuture(client->close());
    waitFuture(server->close());
}

TEST_CASE(reconnect_session) {
    Loop loop;
    auto server = createEchoServer(loop, 9315, {{"socketObjectTimeout", 3000}});
    Signal<std::string> serverDisconnectSignal;
    Signal<std::string> serverReconnectSignal;
    std::atomic<int> connectCount{0};
    server->on("connect", [&](std::shared_ptr<SocketObject> socketObject) {
        connectCount++;
        socketObject->on("disconnect", [&](std::shared_ptr<SocketObject>, std::string msg) {
            serverDisconnectSignal.set(msg);
        });
        socketObject->on("reconnect", [&](std::shared_ptr<SocketObject> socketObject) {
            serverReconnectSignal.set(socketObject->getID());
        });
    });
    waitFuture(server->listen());

    auto client = Client::create(loop.get(), {url(9315)}, {{"name", "Bob"}});
    auto socketObject = waitFuture(client->getSocketObjectReady());
    std::string ID = socketObject->getID();
    CHECK(!ID.empty());
    CHECK_EQ(socketObject->getQueryVal("configure")["socketObjectTimeout"].get<int>(), 3000);
    Signal<std::string> clientReconnectSignal;
    client->on("reconnect", [&](std::shared_ptr<SocketObject> socketObject) {
        clientReconnectSignal.set(socketObject->getID());
    });
    client->disconnect("test");
    CHECK_EQ(serverDisconnectSignal.wait(), std::string("test"));
    CHECK_EQ(clientReconnectSignal.wait(), ID);
    CHECK_EQ(serverReconnectSignal.wait(), ID);
    CHECK_EQ(connectCount.load(), 1);
    auto socketData = waitFuture(client->sendReady("hello", {}));
    CHECK_EQ(socketData->getResDataVal("welcome").get<std::string>(), std::string("Hello Bob"));
    CHECK(client->getSocketObject() == socketObject);
    waitFuture(client->close());
    waitFuture(server->close());
}

TEST_CASE(session_timeout) {
    Loop loop;
    auto server = createEchoServer(loop, 9316);
    Signal<std::string> serverDestroySignal;
    server->on("connect", [&](std::shared_ptr<SocketObject> socketObject) {
        socketObject->on("destroy", [&](std::shared_ptr<SocketObject>, std::string msg) {
            serverDestroySignal.set(msg);
        });
    });
    waitFuture(server->listen());
    auto client = Client::create(loop.get(), {url(9316)});
    auto socketObject = waitFuture(client->getSocketObjectReady());
    std::string ID = socketObject->getID();
    Signal<std::string> clientDestroySignal;
    Signal<std::string> clientConnectSignal;
    client->on("destroy", [&](std::shared_ptr<SocketObject>, std::string msg) {
        clientDestroySignal.set(msg);
    });
    client->on("connect", [&](std::shared_ptr<SocketObject> socketObject) {
        clientConnectSignal.set(socketObject->getID());
    });
    client->disconnect("test");
    CHECK_EQ(clientDestroySignal.wait(), std::string("socketObjectTimeout"));
    CHECK_EQ(serverDestroySignal.wait(), std::string("socketObjectTimeout"));
    std::string newID = clientConnectSignal.wait(5000);
    CHECK(newID != ID);
    waitFuture(client->close());
    waitFuture(server->close());
}

TEST_CASE(client_close) {
    Loop loop;
    auto server = createEchoServer(loop, 9317, {{"socketObjectTimeout", 30000}});
    Signal<std::string> serverDestroySignal;
    server->on("connect", [&](std::shared_ptr<SocketObject> socketObject) {
        socketObject->on("destroy", [&](std::shared_ptr<SocketObject>, std::string msg) {
            serverDestroySignal.set(msg);
        });
    });
    waitFuture(server->listen());
    auto client = Client::create(loop.get(), {url(9317)});
    waitFuture(client->getSocketObjectReady());
    Signal<std::string> clientDestroySignal;
    client->on("destroy", [&](std::shared_ptr<SocketObject>, std::string msg) {
        clientDestroySignal.set(msg);
    });
    waitFuture(client->close());
    CHECK_EQ(clientDestroySignal.wait(), std::string("end"));
    CHECK_EQ(serverDestroySignal.wait(), std::string("end"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    CHECK(server->getSocketObjectList().empty());
    CHECK(!client->getSocketObject());
    CHECK_EQ(test::getErrorOf([&] { waitFuture(client->sendReady("hello", {})); }), std::string("close"));

    auto pendingClient = Client::create(loop.get(), {"ws://127.0.0.1:9399"});
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    waitFuture(pendingClient->close(), 3000);
    waitFuture(server->close());
}

TEST_CASE(server_close) {
    Loop loop;
    auto server = createEchoServer(loop, 9318, {{"socketObjectTimeout", 500}});
    Signal<bool> closeSignal;
    server->on("close", [&] {
        closeSignal.set(true);
    });
    waitFuture(server->listen());
    auto client = Client::create(loop.get(), {url(9318)});
    auto socketObject = waitFuture(client->getSocketObjectReady());
    std::string ID = socketObject->getID();
    Signal<std::string> clientDisconnectSignal;
    Signal<std::string> clientDestroySignal;
    client->on("disconnect", [&](std::shared_ptr<SocketObject>, std::string msg) {
        clientDisconnectSignal.set(msg);
    });
    client->on("destroy", [&](std::shared_ptr<SocketObject>, std::string msg) {
        clientDestroySignal.set(msg);
    });
    waitFuture(server->close());
    CHECK(closeSignal.wait());
    CHECK(!server->isListen());
    CHECK(server->getSocketObjectList().empty());
    CHECK_EQ(clientDisconnectSignal.wait(), std::string("close"));
    CHECK_EQ(clientDestroySignal.wait(), std::string("socketObjectTimeout"));

    Signal<std::string> reconnectSignal;
    client->on("connect", [&](std::shared_ptr<SocketObject> socketObject) {
        reconnectSignal.set(socketObject->getID());
    });
    waitFuture(server->listen());
    CHECK(reconnectSignal.wait(5000) != ID);
    auto socketData = waitFuture(client->sendReady("hello", {}));
    CHECK(socketData->getResDataVal("welcome").is_string());
    waitFuture(client->close());
    waitFuture(server->close());
}

TEST_CASE(listen_error) {
    Loop loop;
    auto server = createEchoServer(loop, 9319);
    waitFuture(server->listen());
    auto otherServer = createEchoServer(loop, 9319);
    Signal<std::string> errorSignal;
    otherServer->on("error", [&](std::string error) {
        errorSignal.set(error);
    });
    std::string error = test::getErrorOf([&] { waitFuture(otherServer->listen()); });
    CHECK(!error.empty());
    CHECK_EQ(errorSignal.wait(), error);
    CHECK(!otherServer->isListen());
    waitFuture(server->close());
    boost::asio::io_context ioContext;
    CHECK_EQ(test::getErrorOf([&] { Client::create(ioContext, {}); }), std::string("Invalid urlList"));
}

TEST_CASE(wss) {
    Loop loop;
    auto certificate = createCertificate("a", "localhost");
    auto server = createEchoServer(loop, 9320, {{"https", {{"cert", certificate.certPath}, {"key", certificate.keyPath}}}, {"perMessageDeflate", true}});
    CHECK(server->isHttps());
    waitFuture(server->listen());

    auto client = Client::create(loop.get(), {url(9320, "wss")}, {{"name", "Bob"}}, {{"ssl", {{"caFile", certificate.certPath}}}});
    auto socketData = waitFuture(client->sendReady("hello", {{"text", std::string(4096, 'x')}}));
    CHECK_EQ(socketData->getResDataVal("welcome").get<std::string>(), std::string("Hello Bob"));
    CHECK_EQ(socketData->getResData().getJSON()["echo"]["text"].get<std::string>().size(), std::size_t(4096));

    auto strictClient = Client::create(loop.get(), {url(9320, "wss")});
    Signal<std::string> errorSignal;
    strictClient->on("ws|error", [&](std::string error) {
        errorSignal.set(error);
    });
    CHECK(!errorSignal.wait().empty());
    waitFuture(strictClient->close());

    auto renewCertificate = createCertificate("b", "localhost");
    server->setHttps({{"cert", renewCertificate.certPath}, {"key", renewCertificate.keyPath}});
    auto renewClient = Client::create(loop.get(), {url(9320, "wss")}, {}, {{"ssl", {{"caFile", renewCertificate.certPath}}}});
    socketData = waitFuture(renewClient->sendReady("hello", {}));
    CHECK(socketData->getResDataVal("welcome").is_string());

    auto insecureContext = std::make_shared<boost::asio::ssl::context>(boost::asio::ssl::context::tls_client);
    insecureContext->set_verify_mode(boost::asio::ssl::verify_none);
    auto insecureClient = Client::create(loop.get(), {url(9320, "wss")}, {}, {}, insecureContext);
    socketData = waitFuture(insecureClient->sendReady("file", Binary{1}));
    CHECK((socketData->getResData().getBinary() == Binary{2}));

    waitFuture(client->close());
    waitFuture(renewClient->close());
    waitFuture(insecureClient->close());
    waitFuture(server->close());
    for (const auto& item : {certificate, renewCertificate}) {
        std::remove(item.certPath.c_str());
        std::remove(item.keyPath.c_str());
    }
}

int main(int argc, char** argv) {
    auto& testMap = test::getTestMap();
    std::vector<std::string> nameList;
    if (argc > 1) {
        for (int i = 1; i < argc; i++) {
            nameList.push_back(argv[i]);
        }
    } else {
        for (const auto& item : testMap) {
            nameList.push_back(item.first);
        }
    }
    int failedCount = 0;
    for (const auto& name : nameList) {
        auto it = testMap.find(name);
        if (it == testMap.end()) {
            std::cerr << "Unknown test " << name << std::endl;
            failedCount++;
            continue;
        }
        try {
            it->second();
            std::cout << "PASS " << name << std::endl;
        } catch (const std::exception& e) {
            std::cout << "FAIL " << name << ": " << e.what() << std::endl;
            failedCount++;
        }
    }
    return failedCount ? 1 : 0;
}
