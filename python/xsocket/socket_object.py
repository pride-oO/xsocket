import asyncio

from . import helpers
from .data import Data
from .helpers import EventEmitter, XSocketError


class SocketObject(EventEmitter):

    def __init__(self, serverConfigure=None, req=None):
        super().__init__()
        self.setMaxListeners(0)

        self._serverConfigure = serverConfigure
        self._req             = req if isinstance(req, dict) else {}
        self._ws              = None
        self._ID              = ''
        self._sign            = ''
        self._destroy         = None
        self._isNewConnection = True
        self._socketDataMap   = {}
        self._socketDataInc   = 0
        self._query           = {}
        self._timeOffset      = 0

        self.on('destroy', self.__onDestroy)
        self.on('ws|disconnect', self.__onWsDisconnect)
        self.on('ws|connect', self.__onWsConnect)
        self.on('ws|message', self.__onWsMessage)

    def getID(self):
        return self._ID or ''

    def getSign(self):
        return self._sign or ''

    def getTime(self):
        return helpers.getTime() + self._timeOffset

    def getTimeOffset(self):
        return self._timeOffset

    def setTimeOffset(self, timeOffset):
        if not helpers.isNumber(timeOffset):
            return False
        self._timeOffset = round(timeOffset)
        return True

    def getReq(self):
        return self._req

    def getQuery(self):
        return self._query

    def getQueryVal(self, key, default=None):
        return self.getQuery().get(key) or default

    def getReqHeaders(self):
        return self.getReq().get('headers') or {}

    def getOrigin(self):
        return self.getReqHeaders().get('origin') or 'unknown'

    def getIP(self):
        return str(self.getReq().get('remoteAddress') or 'unknown')

    def getSocketDataList(self):
        return self._socketDataMap

    def getSocketData(self, ID):
        return self._socketDataMap.get(ID, False)

    def isConnection(self):
        return self._ws.isConnection() if self._ws else False

    def getConnection(self):
        return self._ws if self.isConnection() else False

    def getDestroy(self):
        return self._destroy or False

    def isDestroy(self):
        return bool(self.getDestroy())

    def isServer(self):
        return self._serverConfigure is not None

    async def getWsReady(self):
        if self.getConnection():
            return self.getConnection()
        future = asyncio.get_running_loop().create_future()

        def destroyHandler(socketObject, msg):
            self.off('connect', connectHandler)
            if not future.done():
                future.set_exception(XSocketError('destroy'))

        def connectHandler(socketObject):
            self.off('destroy', destroyHandler)
            if future.done():
                return
            if self.getConnection():
                future.set_result(self.getConnection())
            else:
                future.set_exception(XSocketError('Connection failed'))

        self.once('connect', connectHandler)
        self.once('destroy', destroyHandler)
        return await future

    def send(self, type, data=None, ttl=None):
        self._socketDataInc += 1
        socketData = Data(True, {
            'ID'   : str(self._socketDataInc),
            'type' : type,
            'data' : data,
            'ttl'  : ttl,
            'time' : self.getTime()
        }, self)
        if socketData.getTTL():
            self._socketDataMap[socketData.getID()] = socketData
            socketData.once('destroy', lambda msg: self._socketDataMap.pop(socketData.getID(), None))
        helpers.promise(self.__sendData(socketData))
        return helpers.resolve(socketData)

    async def __sendData(self, socketData):
        try:
            ws = await self.getWsReady()
        except Exception as e:
            socketData.destroy(str(e) or 'Send failed')
            return
        if not ws.sendObject([
            helpers.MSG_SD_CREATE,
            int(socketData.getID()),
            socketData.getType(),
            socketData.getTTL(),
            socketData.getData(),
            socketData.getTime()
        ]):
            socketData.destroy('Send failed')
            return
        socketData.setStatus(Data.STATUS_SEND if socketData.getTTL() else Data.STATUS_RESPONSE)

    def sendReadyResponse(self, type, data=None, ttl=None):
        return helpers.promise(self.__sendReady(type, data, ttl))

    sendReady = sendReadyResponse

    async def __sendReady(self, type, data, ttl):
        socketData = await self.send(type, data, ttl)
        if not socketData.isResponse() and not socketData.getDestroy():
            future = asyncio.get_running_loop().create_future()
            socketData.once('response', lambda resData, error: future.done() or future.set_result(None))
            await future
        if socketData.getError():
            raise XSocketError(socketData.getError())
        return socketData

    def destroy(self, msg=None):
        if self.isDestroy():
            return False
        self._destroy = str(msg or 'destroy')
        self.disconnect(self._destroy)
        self.emit('destroy', self, self._destroy)
        self.clearAllListeners()
        return True

    def disconnect(self, msg=None):
        ws = self.getConnection()
        if not ws:
            return False
        return ws.closeConnection(msg)

    def update(self, ws):
        if not isinstance(ws, helpers.Connection):
            return False
        if not ws.isConnection() or self._destroy:
            return False
        query = ws.getQuery()
        if self._ID != query.get('xSOId') or self._sign != query.get('xSOSign'):
            self._ID    = query.get('xSOId')
            self._sign  = query.get('xSOSign')
            self._query = {key: val for key, val in query.items() if key not in ('xSOId', 'xSOSign', 'xV')}
            self.emit('update', self)
        if self.isServer():
            ws.sendObject([
                helpers.MSG_SO_UPDATE,
                self.getID(),
                self.getSign(),
                self._serverConfigure if isinstance(self._serverConfigure, dict) else {},
                helpers.VERSION,
                helpers.getTime()
            ])
        else:
            self.setTimeOffset(ws.getTimeOffset())
            ws.sendObject([helpers.MSG_SO_TIME, helpers.getTime()])
        self._ws = ws
        self.emit('ws|connect', ws)
        return True

    def __onDestroy(self, socketObject, msg):
        for socketData in list(self._socketDataMap.values()):
            socketData.destroy('SocketObject|destroy')

    def __onWsDisconnect(self, msg):
        msg = msg if isinstance(msg, str) else 'unknown'
        self.emit('disconnect', self, msg)
        if msg == 'end':
            self.destroy('end')

    def __onWsConnect(self, ws):
        self.emit('connect', self)
        if not self._isNewConnection:
            self.emit('reconnect', self)
        else:
            self._isNewConnection = False

    def __onWsMessage(self, body):
        body = list(body) + [None] * (6 - len(body))
        if body[0] == helpers.MSG_SO_TIME:
            return self.__onWsTime(body)
        ID = body[1]
        if not helpers.isInt(ID) or ID < 1:
            return
        ID = int(ID)
        if body[0] == helpers.MSG_SD_CREATE:
            if not isinstance(body[2], str) or not body[2] or not (body[4] is None or helpers.isObject(body[4])):
                return
            socketData = Data(False, {
                'ID'   : str(ID),
                'type' : body[2],
                'ttl'  : body[3],
                'data' : body[4],
                'time' : body[5]
            }, self)
            self.emit('data', socketData)
            self.emit('data|' + socketData.getType(), socketData)
            if not socketData.getTTL():
                socketData.setStatus(Data.STATUS_RESPONSE)
        elif body[0] == helpers.MSG_SD_UPDATE:
            socketData = self.getSocketData(str(ID))
            if socketData:
                socketData.setResData(body[2])
                socketData.setError(body[3] or False)
                socketData.setStatus(Data.STATUS_RESPONSE)

    def __onWsTime(self, body):
        ws = self.getConnection()
        if not ws or not helpers.isNumber(body[1]):
            return
        if self.isServer():
            ws.sendObject([helpers.MSG_SO_TIME, body[1], helpers.getTime()])
        elif helpers.isNumber(body[2]):
            now = helpers.getTime()
            self.setTimeOffset(body[2] + (now - body[1]) / 2 - now)
