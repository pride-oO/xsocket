import asyncio
from urllib.parse import quote, urlencode

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from . import helpers
from .helpers import Connection, EventEmitter, XSocketError, logger
from .socket_object import SocketObject


class Client(EventEmitter):

    def __init__(self, serverUrl, query=None, settings=None):
        super().__init__()
        self.setMaxListeners(0)

        if isinstance(serverUrl, str):
            self._urlList = [serverUrl]
        elif isinstance(serverUrl, (list, tuple)):
            self._urlList = list(serverUrl)
        else:
            self._urlList = []
        if not self._urlList:
            raise XSocketError('Invalid urlList')
        self._query           = dict(query) if isinstance(query, dict) else {}
        self._settings        = dict(settings) if isinstance(settings, dict) else {}
        self._ws              = None
        self._socketObject    = None
        self._isVersionWarned = False
        self._isClose         = False

        self.on('connect', self.__onConnect)
        self._runTask = helpers.promise(self.__run())

    def getSettings(self):
        return self._settings

    def getSettingsVal(self, key, default=None):
        return self.getSettings().get(key) or default

    def getSocketObject(self):
        return self._socketObject or False

    async def getSocketObjectReady(self):
        if self.getSocketObject():
            return self.getSocketObject()
        future = asyncio.get_running_loop().create_future()
        self.once('connect', lambda socketObject: future.done() or future.set_result(socketObject))
        return await future

    def send(self, type, data=None, ttl=None):
        return helpers.promise(self.__send(type, data, ttl))

    async def __send(self, type, data, ttl):
        socketObject = await self.getSocketObjectReady()
        return await socketObject.send(type, data, ttl)

    def sendReady(self, type, data=None, ttl=None):
        return helpers.promise(self.__sendReady(type, data, ttl))

    sendReadyResponse = sendReady

    async def __sendReady(self, type, data, ttl):
        socketObject = await self.getSocketObjectReady()
        return await socketObject.sendReady(type, data, ttl)

    def disconnect(self, msg=None):
        socketObject = self.getSocketObject()
        if not socketObject:
            return False
        return socketObject.disconnect(msg)

    async def close(self, msg='end'):
        self._isClose = True
        if not self._ws or not self._ws.closeConnection(msg):
            self._runTask.cancel()
        await asyncio.gather(self._runTask, return_exceptions=True)
        if self._socketObject:
            self._socketObject.destroy(msg)

    def __getUrl(self):
        url = self._urlList[helpers.getRandInt(0, len(self._urlList) - 1)]
        query = dict(self._query)
        query['xV'] = helpers.VERSION
        if self._socketObject and self._socketObject.getID() and self._socketObject.getSign():
            query['xSOId']   = self._socketObject.getID()
            query['xSOSign'] = self._socketObject.getSign()
        return url + ('&' if '?' in url else '?') + urlencode({str(key): str(val) for key, val in query.items()}, quote_via=quote)

    async def __run(self):
        while not self._isClose:
            try:
                websocket = await connect(self.__getUrl(), ssl=self.getSettingsVal('ssl'), max_size=helpers.MAX_MESSAGE_SIZE)
            except Exception as e:
                self.emit('ws|error', str(e))
                await asyncio.sleep(1)
                continue
            await self.__handleConnection(websocket)
            if not self._isClose:
                await asyncio.sleep(1)

    async def __handleConnection(self, websocket):
        ws = Connection(websocket)
        self._ws = ws
        if self._isClose:
            ws.closeConnection('end')
        self.emit('ws|connect', ws)
        msgInc = 0

        async def pongCheck():
            nonlocal msgInc
            while True:
                await asyncio.sleep(21)
                if not msgInc:
                    ws.closeConnection('pingTimeout')
                msgInc = 0

        pongTask = helpers.promise(pongCheck())
        try:
            async for message in websocket:
                msgInc += 1
                if message == 'ping':
                    ws.sendMessage('pong')
                    continue
                try:
                    self.__onMessage(ws, message)
                except Exception:
                    logger.exception('Message handler error')
        except ConnectionClosed:
            pass
        finally:
            pongTask.cancel()
            await websocket.wait_closed()
            ws.stop()
            self._ws = None
            msg = websocket.close_reason or 'close'
            self.emit('ws|disconnect', msg)
            if self._socketObject:
                self._socketObject.emit('ws|disconnect', msg)

    def __onMessage(self, ws, message):
        body = helpers.decodeMessage(message)
        if not body:
            return
        if body[0] != helpers.MSG_SO_UPDATE:
            if self._socketObject:
                self._socketObject.emit('ws|message', body)
            return
        body = body + [None] * (6 - len(body))
        if body[4] != helpers.VERSION and not self._isVersionWarned:
            self._isVersionWarned = True
            logger.warning('Version mismatch client %s, server %s', helpers.VERSION, body[4] or 'unknown')
        ws.setTimeOffset(body[5] - helpers.getTime() if helpers.isNumber(body[5]) else 0)
        ws.setQuery({
            'xSOId'     : str(body[1]),
            'xSOSign'   : str(body[2]),
            'configure' : body[3] if isinstance(body[3], dict) else {}
        })
        if self._socketObject:
            query = ws.getQuery()
            if query['xSOId'] == self._socketObject.getID() and query['xSOSign'] == self._socketObject.getSign() and self._socketObject.update(ws):
                return
            self._socketObject.destroy('update')
            self._socketObject = None
        socketObject = self._socketObject = SocketObject()
        socketObject.once('connect', lambda socketObject: self.emit('connect', socketObject))
        socketObject.on('destroy', self.__onSocketObjectDestroy)
        socketObject.on('disconnect', lambda socketObject, msg: self.emit('disconnect', socketObject, msg))
        socketObject.on('reconnect', lambda socketObject: self.emit('reconnect', socketObject))
        socketObject.on('data', lambda socketData: self.emit('data', socketData))
        if not socketObject.update(ws):
            ws.closeConnection('update')

    def __onSocketObjectDestroy(self, socketObject, msg):
        if self._socketObject is socketObject:
            self._socketObject = None
        self.emit('destroy', socketObject, msg)

    def __onConnect(self, socketObject):
        loop = asyncio.get_running_loop()
        timerMap = {'destroy' : None}

        def setDestroyTimeout(action, ttl=0):
            if timerMap['destroy']:
                timerMap['destroy'].cancel()
                timerMap['destroy'] = None
            if action:
                timerMap['destroy'] = loop.call_later(max(ttl, 0) / 1000, socketObject.destroy, 'socketObjectTimeout')

        def disconnectHandler(socketObject, msg):
            configure = socketObject.getQueryVal('configure', {})
            setDestroyTimeout(True, helpers.toInt(configure.get('socketObjectTimeout') if isinstance(configure, dict) else 0))

        socketObject.on('reconnect', lambda socketObject: setDestroyTimeout(False))
        socketObject.on('disconnect', disconnectHandler)
        socketObject.once('destroy', lambda socketObject, msg: setDestroyTimeout(False))
