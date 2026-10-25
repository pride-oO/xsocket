rfr# xSocket for Python

WebSocket client and server for Python (asyncio). Same protocol as [xSocket](https://github.com/pride-oO/xsocket) for Node.js and the browser: a Python server works with JS clients, a Python client works with a Node.js server.

- Request-response with TTL (`sendReady`), clock synchronization with the server
- Automatic reconnect and session recovery within `socketObjectTimeout`
- Auth hook on the server (`use('auth')`)
- `ws://` or `wss://` depending on certificate, optional `perMessageDeflate` compression
- Binary messages: `bytes`, `bytearray` or `memoryview` as data, received as `bytes`
- Python 3.9+, depends on [websockets](https://pypi.org/project/websockets/)

```
pip install xsocket
```

The API mirrors the JS version. Methods that return a `Promise` in JS (`send`, `sendReady`, `response`) return an awaitable task here: the message is sent even without `await`. Event handlers can be regular functions or coroutines and receive the same arguments as in JS. `Client` and `Server` must be created inside a running event loop.

## Server:
```python
import asyncio
import xsocket

async def main():
    xServer = xsocket.Server({
        'host' : '0.0.0.0', # (Not necessary) Host listen (Default: all interfaces)
        'port' : 8752, # (Not necessary) Port listen (Default: 443 with certificate, 80 without)
        'socketObjectTimeout' : 30000, # (Not necessary) Allows the user to reconnect within 30 seconds (Default: 0)
        'perMessageDeflate' : True, # (Not necessary) Compress messages (Default: False)
        'https' : { # (Not necessary) Without certificate server uses ws:// instead of wss://
            'cert' : '<path>/cert.pem',
            'key' : '<path>/private.key'
        }
    })

    # (Not necessary) Auth, by default all connections are allowed. Can be async
    def auth(xSocketObject, success, failed):
        if xSocketObject.getQueryVal('name'):
            return success()
        failed('Access denied')

    xServer.use('auth', auth)

    def onConnect(xSocketObject):
        print('Connect event:', xSocketObject.getID(), xSocketObject.getIP(), xSocketObject.getOrigin(), xSocketObject.getQuery(), xSocketObject.getReqHeaders())

        async def onData(xSocketData):
            print('Data event', xSocketData.getID(), xSocketData.getType(), xSocketData.getData())
            await asyncio.sleep(1)
            try:
                await xSocketData.response({'welcome' : 'Hello ' + xSocketObject.getQueryVal('name')})
                # OR
                # await xSocketData.response({}, 'Permision denied')
            except xsocket.XSocketError as e:
                print('Response send failed', xSocketData.getID(), e)

        xSocketObject.on('data', onData)
        xSocketObject.on('disconnect', lambda xSocketObject, msg: print('Disconnect event:', xSocketObject.getID(), msg))
        xSocketObject.on('reconnect', lambda xSocketObject: print('Reconnect event:', xSocketObject.getID()))
        xSocketObject.on('destroy', lambda xSocketObject, msg: print('Destroy event:', xSocketObject.getID(), msg))

    xServer.on('connect', onConnect)
    xServer.on('listen', lambda: print('Listen event', xServer.getPort()))

    await xServer.listen()

    # Stop: destroys all SocketObjects, closes connections, emits 'close'
    # await xServer.close()

    # Port and certificate changes apply on the next listen()
    # xServer.setHost('127.0.0.1')
    # xServer.setPort(8753)
    # xServer.setHttps({'cert' : '<path>/cert.pem', 'key' : '<path>/private.key'}) # OR setHttps({}) for ws://
    # await xServer.listen()

    # Renew certificate without restart (only if server already runs with certificate)
    # xServer.setHttps({'cert' : '<path>/new_cert.pem', 'key' : '<path>/new_private.key'})

    await asyncio.Future()

asyncio.run(main())
```

## Client:
```python
import asyncio
import xsocket

async def main():
    xClient = xsocket.Client(['wss://server_domain.com:8752'], {'name' : 'Bob'}) # ws:// if server without certificate
    # Self-signed certificate: xsocket.Client(url, query, {'ssl' : ssl.SSLContext})

    xClient.on('reconnect', lambda xSocketObject: print('reconnect event', xSocketObject.getID()))
    xClient.on('disconnect', lambda xSocketObject, msg: print('disconnect', xSocketObject.getID(), msg))
    xClient.on('destroy', lambda xSocketObject, msg: print('destroy', xSocketObject.getID(), msg))

    # Messages from server
    xClient.on('data', lambda xSocketData: xSocketData.response({'ok' : True}))

    xSocketObject = await xClient.getSocketObjectReady()
    print('connect event:', xSocketObject.getID())

    try:
        xSocketData = await xClient.sendReady(
            'helloServer', # Name message
            {'message' : 'Hello server'}, # Data message
            2000 # TTL ready response (Default: 3s)
        )
        print('Server responsed #' + xSocketData.getID(), xSocketData.getType(), xSocketData.getResData())
    except xsocket.XSocketError as e:
        print('Server responsed error', e)

    # Binary message, response can be binary too
    # xSocketData = await xClient.sendReady('file', b'\x01\x02\x03')

    # Without waiting for response
    # await xClient.send('log', {'message' : 'Hello'}, 0)

    # Close connection and destroy session on server
    await xClient.close()

asyncio.run(main())
```
