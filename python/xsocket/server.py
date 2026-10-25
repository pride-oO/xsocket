import asyncio
import inspect
import ssl
import uuid
from urllib.parse import parse_qsl

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from . import helpers
from .helpers import Connection, EventEmitter, XSocketError, logger
from .socket_object import SocketObject


class Server(EventEmitter):

    def __init__(self, settings=None):
        super().__init__()
        self.setMaxListeners(0)
        self._settings          = dict(settings or {})
        self._server            = None
        self._isListen          = False
        self._useCallbackMap    = {}
        self._socketObjectMap   = {}
        self._versionWarnMap    = {}
        self._sslContext        = None

        self._socketObjectTimeout = helpers.toInt(self.getSettingsVal('socketObjectTimeout', 0), -1)
        if self._socketObjectTimeout < 0:
            self._socketObjectTimeout = 0
            logger.warning('socketObjectTimeout invalid value, set default is 0')

    def getSocketObjectList(self):
        return self._socketObjectMap

    def getSocketObject(self, ID):
        return self._socketObjectMap.get(ID, False)

    def getPort(self):
        return self.getSettings().get('port') or (443 if self.isHttps() else 80)

    def isHttps(self):
        return bool(self.getSettingsVal('https', {}).get('cert'))

    def isListen(self):
        return self._isListen is True

    def getHost(self):
        return self.getSettingsVal('host')

    def setHost(self, host):
        return self.setSettingsVal('host', host)

    def setPort(self, port):
        return self.setSettingsVal('port', port)

    def setHttps(self, options):
        self.setSettingsVal('https', options)
        if self.isListen() and self.isHttps() and self._sslContext:
            self._sslContext.load_cert_chain(options['cert'], options.get('key'))
        return self

    def setSettingsVal(self, key, val):
        self._settings[key] = val
        return self

    def getSettings(self):
        return self._settings

    def getSettingsVal(self, key, default=None):
        return self.getSettings().get(key) or default

    async def useEmit(self, target, data):
        callback = self._useCallbackMap.get(target)
        if not callable(callback):
            raise XSocketError('callbackNotFound')
        future = asyncio.get_running_loop().create_future()

        def successCallback(*args):
            if not future.done():
                future.set_result(args[0] if args else None)

        def failedCallback(message=None):
            if not future.done():
                future.set_exception(XSocketError(message if isinstance(message, str) else ''))

        def onCallbackDone(task):
            if not task.cancelled() and task.exception():
                failedCallback(str(task.exception()))

        try:
            result = callback(data, successCallback, failedCallback)
            if inspect.isawaitable(result):
                helpers.promise(result).add_done_callback(onCallbackDone)
        except Exception as e:
            failedCallback(str(e))
        try:
            return await asyncio.wait_for(future, 10)
        except asyncio.TimeoutError:
            raise XSocketError('timeout')

    def use(self, target, callback):
        if not isinstance(target, str) or not target:
            raise XSocketError('Invalid target')
        if not callable(callback):
            raise XSocketError('Invalid callback')
        self._useCallbackMap[target] = callback
        self.emit('use|append', target, callback)
        return self

    async def listen(self):
        if self._isListen:
            raise XSocketError('repeat')
        if self._isListen is None:
            future = asyncio.get_running_loop().create_future()
            self.once('listen', lambda: future.done() or future.set_result(None))
            self.once('error', lambda e: future.done() or future.set_exception(e))
            return await future
        self._isListen = None
        perMessageDeflate = self.getSettingsVal('perMessageDeflate', False)
        try:
            self._sslContext = None
            if self.isHttps():
                options = self.getSettingsVal('https', {})
                self._sslContext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                self._sslContext.load_cert_chain(options['cert'], options.get('key'))
            self._server = await serve(
                self.__onConnection,
                self.getHost(),
                self.getPort(),
                ssl=self._sslContext,
                compression='deflate' if perMessageDeflate else None,
                max_size=helpers.MAX_MESSAGE_SIZE
            )
        except Exception as e:
            self._isListen = False
            self.emit('error', e)
            raise
        self._isListen = True
        self.emit('listen')

    async def close(self):
        for socketObject in list(self._socketObjectMap.values()):
            socketObject.destroy('close')
        if self._server:
            server = self._server
            self._server = None
            self._isListen = False
            server.close()
            await server.wait_closed()
            self.emit('close')
        self._isListen = False

    async def __onConnection(self, websocket):
        request = websocket.request
        query = dict(parse_qsl(request.path.partition('?')[2], keep_blank_values=True))
        ws = Connection(websocket, query)
        remoteAddress = websocket.remote_address
        req = {
            'path'          : request.path,
            'headers'       : {key.lower(): val for key, val in request.headers.raw_items()},
            'remoteAddress' : remoteAddress[0] if isinstance(remoteAddress, tuple) else remoteAddress
        }

        clientVersion = str(query.get('xV') or 'unknown')
        if clientVersion != helpers.VERSION and clientVersion not in self._versionWarnMap:
            self._versionWarnMap[clientVersion] = True
            logger.warning('Version mismatch server %s, client %s', helpers.VERSION, clientVersion)

        socketObject = None
        query['xSOId']   = str(query.get('xSOId') or '')
        query['xSOSign'] = str(query.get('xSOSign') or '')
        if query['xSOId'] and query['xSOSign']:
            s = self.getSocketObject(query['xSOId'])
            if s and s.getSign() == query['xSOSign'] and s.update(ws):
                socketObject = s
        if not socketObject:
            query['xSOId']   = str(uuid.uuid4())
            query['xSOSign'] = str(uuid.uuid4())
            socketObject = SocketObject({'socketObjectTimeout' : self._socketObjectTimeout}, req)
            if not self.getSettingsVal('auth', False) and not callable(self._useCallbackMap.get('auth')):
                self.use('auth', lambda socketObject, successCallback, failedCallback: successCallback())
            if socketObject.update(ws):
                helpers.promise(self.__auth(socketObject))
            else:
                ws.closeConnection('update')

        msgInc = 0

        async def ping():
            nonlocal msgInc
            countPing = 0
            while True:
                await asyncio.sleep(5)
                if countPing > 4:
                    if not msgInc:
                        ws.closeConnection('pingTimeout')
                        return
                    msgInc = 0
                    countPing = 0
                ws.sendMessage('ping')
                countPing += 1

        pingTask = helpers.promise(ping())
        self.emit('ws|connect', ws)
        try:
            async for message in websocket:
                msgInc += 1
                if message == 'pong' or not socketObject:
                    continue
                try:
                    body = helpers.decodeMessage(message)
                    if body:
                        socketObject.emit('ws|message', body)
                except Exception:
                    logger.exception('Message handler error')
        except ConnectionClosed:
            pass
        finally:
            pingTask.cancel()
            await websocket.wait_closed()
            ws.stop()
            msg = websocket.close_reason or 'close'
            socketObject.emit('ws|disconnect', msg)
            self.emit('ws|disconnect', msg)

    async def __auth(self, socketObject):
        try:
            await self.useEmit('auth', socketObject)
        except Exception as e:
            socketObject.destroy(str(e) or 'deniedAuth')
            return
        if socketObject.isDestroy():
            return
        self._socketObjectMap[socketObject.getID()] = socketObject
        self.emit('connect', socketObject)
        loop = asyncio.get_running_loop()
        timerMap = {'destroy' : None}

        def cancelTimeout(*args):
            if timerMap['destroy']:
                timerMap['destroy'].cancel()
                timerMap['destroy'] = None

        def disconnectHandler(socketObject, msg):
            cancelTimeout()
            timerMap['destroy'] = loop.call_later(self._socketObjectTimeout / 1000, socketObject.destroy, 'socketObjectTimeout')
            socketObject.once('connect', cancelTimeout)

        def destroyHandler(socketObject, msg):
            self._socketObjectMap.pop(socketObject.getID(), None)
            cancelTimeout()

        socketObject.on('disconnect', disconnectHandler)
        socketObject.once('destroy', destroyHandler)
