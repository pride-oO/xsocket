import WebSocket from 'ws';
import xSocket from './index.js';
import Server from './Server.js';

xSocket.window    = undefined;
xSocket.process   = typeof process === 'object' ? process : false;
xSocket.WebSocket = WebSocket;
xSocket.Server    = Server;

export default xSocket;
