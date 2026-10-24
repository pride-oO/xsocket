# xSocket

WebSocket client and server for Node.js and the browser.

- Request-response: every message can wait for a reply (`sendReady`) with TTL
- TTL counts from the send time on both sides, client clock is synchronized with the server
- `require` and `import` (ESM) support
- Automatic reconnect and session recovery within `socketObjectTimeout`
- Auth hook on the server (`use('auth')`)
- `ws://` or `wss://` depending on certificate, optional `perMessageDeflate` compression
- Binary messages: `ArrayBuffer`, `TypedArray` or `Buffer` as data, received as `Uint8Array` (`Buffer` in Node.js)
- Version check: client and server warn in console if versions differ
- Compact JSON protocol
- Python (asyncio) server and client with the same protocol: [python/README.md](python/README.md)

```
npm install xsocket
```

## Server:
```javascript
// Init
const xSocket = require('xsocket'); // OR import xSocket, { Server } from 'xsocket'

const xServer = new xSocket.Server({
    'port' : 8752, // (Not necessary) Port listen (Default: 443 with certificate, 80 without)
    'socketObjectTimeout' : 30000, // (Not necessary) Allows the user to reconnect within 30 seconds (Default: 0)
    'perMessageDeflate' : true, // (Not necessary) Compress messages larger than 1KB, or ws perMessageDeflate options object (Default: false)
    'https' : { // (Not necessary) Without certificate server uses ws:// instead of wss://
        "cert" : "<path>/cert.pem",
        "key" : "<path>/private.key"
    }
});

// (Not necessary) Auth, by default all connections are allowed
xServer.use('auth', (xSocketObject, success, failed) => {
    if(xSocketObject.getQueryVal('name')){
        return success();
    }
    failed('Access denied');
});

xServer.on('error', (err) => {
    console.log(err);
});

xServer.on('listen', () => {
    console.log('Listen event', xServer.getPort());
});

xServer.on('connect', (xSocketObject) => {

    console.log(
            'Connect event:',
            xSocketObject.getID(), // ID
            xSocketObject.getIP(),  // IP
            xSocketObject.getOrigin(), // Origin url
            xSocketObject.getQuery(), // Clent query (You can transfer data, for example, for authorization)
            xSocketObject.getReqHeaders() // All headers request
    );

    // Data message event
    xSocketObject.on('data', (xSocketData) => {
        console.log(
            'Data event', 
            xSocketData.getID(),   // ID message
            xSocketData.getType(), // Type message
            xSocketData.getData()  // Data
        );

        // Timeout 5s and response
        setTimeout(() => {
            xSocketData.response({'welcome' : 'Hello ' + xSocketObject.getQuery()['name']}, false).then(function (){
                console.log('Response send success', xSocketData.getID());
            }).catch(function (e) {
                console.log('Response send failed', xSocketData.getID(), e.message);
            });
            // OR
            // xSocketData.response({}, 'Permision denied');
        }, 5000);
    });

    // Disconnect event
    xSocketObject.on('disconnect', (xSocketObject, message) => {
        console.log('Disconnect event:', xSocketObject.getID(), message);
    });

    // Reconnect event (Only works if 'socketObjectTimeout' is specified)
    xSocketObject.on('reconnect', (xSocketObject) => {
        console.log('Reconnect event:', xSocketObject.getID());
    });

    // Destroy SocketObject
    xSocketObject.on('destroy', (xSocketObject, msg) => {
        console.log('Destroy event:', xSocketObject.getID(), msg);
    });
});

// Listen
xServer.listen().then(() => {
    console.log('Listen then');
}).catch((e) => {
    console.log('Listen error', e.message);
});
```



## Client (ECMAScript 5+):


```javascript
// Init 
var xSocket = require('xsocket'); // OR import xSocket from 'xsocket' (bundlers use browser build) OR import xSocket from './xSocket.mjs' OR <script src="./xSocket.min.js"></script>

var xClient = new xSocket(['wss://server_domain.com:8752'], {'name' : 'Bob'}); // Or xSocket.Client(), ws:// if server without certificate

// Error event
xClient.on('error', function (err) {
    console.log(err);
});

// Connect event
xClient.on('connect', function (xSocketObject) {
    console.log('connect event:', xSocketObject.getID());


    // Send message 
    xSocketObject.send(
        'helloServer', // Name message
        {'message' : 'Hello server'}, // Data message
        2000 // TTL ready response (Default: 3s)
        ).then(function(xSocketData){
            console.log('Send to server message #'+xSocketData.getID(), xSocketData.getType(), xSocketData.getData());
            
            // Ready response
            xSocketData.on('response', function (resData, error){
                if(error){
                    return console.error('Server responsed error', error);
                }
                console.log('Server responsed #'+xSocketData.getID(), xSocketData.getType(), resData);
            });
    }).catch(function (e){
        console.log('Message not sended, info:', e.message);
    });
    // Binary message, response can be binary too
    // xSocketObject.sendReady('file', new Uint8Array([1, 2, 3])).then(function (xSocketData){
    //     console.log(xSocketData.getResData()); // Uint8Array or object
    // });
    // OR
    //xSocketObject.sendReady(
    //    'helloServer', // Name message
    //    {'message' : 'Hello server'}, // Data message
    //    2000 // TTL ready response (Default: 3s)
    //).then(function (xSocketData){
    //    console.log('Server responsed #'+xSocketData.getID(), xSocketData.getType(), xSocketData.getResData());
    //}).catch(function (e){
    //    console.error('Server responsed error', e.message);
    //});
    
});

// Reconnect event (Only works if 'socketObjectTimeout' is specified)
xClient.on('reconnect', function (xSocketObject) {
    console.log('reconnect event', xSocketObject.getID());
});

// Disconnect event
xClient.on('disconnect', function(xSocketObject, msg) {
    console.log('disconnect', xSocketObject.getID(), msg);
});

// Destroy SocketObject
xClient.on('destroy', function(xSocketObject) {
    console.log('destroy', xSocketObject.getID(), xSocketObject.getSign());
});
```
