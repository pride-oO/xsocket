global.xSocket = function xSocket(serverUrl, query, settings) {
    return new xSocket.Client(serverUrl, query, settings);
};
global.xSocket.window = undefined;
global.xSocket.process = process || false;
global.xSocket.WebSocket = require('ws');