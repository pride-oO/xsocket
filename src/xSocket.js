const xSocket = function xSocket(serverUrl, query, settings){
    return new xSocket.Client(serverUrl, query, settings);
};
xSocket.VERSION   = '2.3.0';
xSocket.window    = typeof window === 'object' ? window : false;
xSocket.WebSocket = typeof WebSocket !== 'undefined' ? WebSocket : false;
xSocket.process   = false;

export default xSocket;
