/**
 * xSocket
 * @licence MIT
 * @author Vadim Pride (pride.mk.ua@gmail.com)
 * @link https://github.com/VadimPride
 */
var xSocket = function xSocket(serverUrl, query, settings){
    return new xSocket.Client(serverUrl, query, settings);
};
xSocket.window = typeof window === 'object' ? window : false;
xSocket.WebSocket = typeof WebSocket === 'function' ? WebSocket : false;
xSocket.process = false;

if(typeof module === 'object' && module.exports){
    module.exports = xSocket;
}else if(xSocket.window){
    window.xSocket = xSocket;
}