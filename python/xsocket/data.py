import asyncio

from . import helpers
from .helpers import EventEmitter, XSocketError


class Data(EventEmitter):

    STATUS_NEW      = 0
    STATUS_SEND     = 1
    STATUS_RESPONSE = 2
    DEFAULT_TTL     = 3000

    def __init__(self, isOutput, dataObject=None, socketObject=None):
        super().__init__()
        self.setMaxListeners(0)
        dataObject = dataObject if isinstance(dataObject, dict) else {}

        ID   = dataObject.get('ID')
        type = dataObject.get('type')
        data = dataObject.get('data')
        ttl  = dataObject.get('ttl')
        time = dataObject.get('time')

        self._isOutput     = bool(isOutput)
        self._socketObject = socketObject
        self._ID           = ID if isinstance(ID, str) else ''
        self._type         = type if isinstance(type, str) else ''
        self._data         = data if helpers.isObject(data) else {}
        self._ttl          = ttl if helpers.isInt(ttl) and ttl >= 0 else Data.DEFAULT_TTL
        now                = socketObject.getTime() if socketObject else helpers.getTime()
        self._time         = time if helpers.isNumber(time) and time > 0 else now
        self._resData      = {}
        self._error        = False
        self._status       = Data.STATUS_NEW
        self._destroy      = False
        self._ttlTimeout   = asyncio.get_running_loop().call_later(
            max(0, min(self._ttl, self._ttl - (now - self._time))) / 1000,
            self.destroy, 'ttl', True
        )

    def getID(self):
        return str(self._ID)

    def getType(self):
        return str(self._type)

    def getTTL(self):
        return self._ttl

    def getTime(self):
        return self._time

    def getData(self):
        return self._data

    def getDataVal(self, key, default=None):
        try:
            return self.getData()[key] or default
        except (KeyError, IndexError, TypeError):
            return default

    def getResData(self):
        return self._resData

    def getResDataVal(self, key, default=None):
        try:
            return self.getResData()[key] or default
        except (KeyError, IndexError, TypeError):
            return default

    def getStatus(self):
        return self._status

    def getDestroy(self):
        return self._destroy or False

    def getSocketObject(self):
        return self._socketObject

    def getError(self):
        return self._error or False

    def isNew(self):
        return self._status == Data.STATUS_NEW

    def isSend(self):
        return self._status == Data.STATUS_SEND

    def isResponse(self):
        return self._status == Data.STATUS_RESPONSE

    def response(self, data=None, error=None):
        return helpers.promise(self.__response(data, error))

    async def __response(self, data, error):
        if self._isOutput:
            raise XSocketError('Output data')
        if self._destroy or self.isResponse() or not self._ttl:
            raise XSocketError('destroy')
        self.setResData(data)
        self.setError(error)
        future = asyncio.get_running_loop().create_future()

        def responseHandler(resData, error):
            if not future.done():
                future.set_result(self)

        def destroyHandler(msg):
            if not future.done():
                future.set_exception(XSocketError(msg))

        self.once('response', responseHandler)
        self.once('destroy', destroyHandler)
        try:
            ws = await self._socketObject.getWsReady()
            if ws.sendObject([
                helpers.MSG_SD_UPDATE,
                int(self._ID),
                self.getResData(),
                self.getError()
            ]):
                self.setStatus(Data.STATUS_RESPONSE)
            else:
                self.destroy('Send failed')
        except Exception as e:
            self.destroy(str(e) or 'Send failed')
        return await future

    def setStatus(self, status):
        if (
            status not in (Data.STATUS_SEND, Data.STATUS_RESPONSE) or
            self._status == Data.STATUS_RESPONSE or
            self._status == status
        ):
            return False
        self._status = status
        self.emit('setStatus', status)
        if status == Data.STATUS_RESPONSE:
            self.emit('response', self.getResData(), self.getError())
            self.destroy('response')
        return True

    def setError(self, errorMsg):
        if not isinstance(errorMsg, str):
            return False
        self._error = errorMsg
        return True

    def setResData(self, data):
        if data is None:
            data = {}
        if not helpers.isObject(data):
            return False
        self._resData = data
        return True

    def destroy(self, msg=None, isSetError=False):
        if self._destroy:
            return False
        self._destroy = msg if isinstance(msg, str) and msg else 'destroy'
        self._ttlTimeout.cancel()
        if isSetError and not self._error:
            self.setError(self._destroy)
        if self._status != Data.STATUS_RESPONSE:
            self.setStatus(Data.STATUS_RESPONSE)
        self.emit('destroy', self._destroy)
        self.clearAllListeners()
        return True
