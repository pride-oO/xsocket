import xSocket from './xSocket.js';
import helpers from './helpers.js';
import EventEmitter from './EventEmitter.js';
import Data from './Data.js';
import SocketObject from './SocketObject.js';
import Client from './Client.js';

xSocket.helpers       = helpers;
xSocket.EventEmitter  = EventEmitter;
xSocket.Data          = Data;
xSocket.xSocketObject = SocketObject;
xSocket.Client        = Client;

export const VERSION = xSocket.VERSION;
export { helpers, EventEmitter, Data, SocketObject, Client };
export default xSocket;
