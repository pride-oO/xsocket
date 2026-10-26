import asyncio
import inspect
import json
import logging
import random
import struct
import time

from websockets.protocol import State

VERSION = '2.3.0'

MSG_SO_UPDATE = 0
MSG_SD_CREATE = 1
MSG_SD_UPDATE = 2
MSG_SO_TIME   = 3

MAX_MESSAGE_SIZE = 100 * 1024 * 1024

logger = logging.getLogger('xsocket')

_taskSet = set()


class XSocketError(Exception):
    pass


def getTime():
    return int(time.time() * 1000)


def getRandInt(min, max):
    return random.randint(min, max)


def isNumber(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == value


def isInt(value):
    return isNumber(value) and value % 1 == 0


def isBinary(data):
    return isinstance(data, (bytes, bytearray, memoryview))


def isObject(data):
    return isinstance(data, (dict, list)) or isBinary(data)


def toInt(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def promise(awaitable):
    task = asyncio.ensure_future(awaitable)
    _taskSet.add(task)
    task.add_done_callback(_onTaskDone)
    return task


def resolve(value):
    future = asyncio.get_running_loop().create_future()
    future.set_result(value)
    return future


def _onTaskDone(task):
    _taskSet.discard(task)
    if not task.cancelled():
        task.exception()


def getMessageDataIndex(msgType):
    return 4 if msgType == MSG_SD_CREATE else 2


def _dumps(body):
    return json.dumps(body, separators=(',', ':'), ensure_ascii=False)


def encodeMessage(body):
    dataIndex = getMessageDataIndex(body[0])
    data = body[dataIndex] if len(body) > dataIndex else None
    if not isBinary(data):
        return _dumps(body)
    body = list(body)
    body[dataIndex] = None
    headerBytes = _dumps(body).encode()
    return struct.pack('>I', len(headerBytes)) + headerBytes + bytes(data)


def decodeMessage(raw):
    try:
        if isinstance(raw, str):
            body = json.loads(raw)
        else:
            headerLength = struct.unpack_from('>I', raw)[0]
            body = json.loads(bytes(raw[4:4 + headerLength]).decode())
            if isinstance(body, list) and body:
                dataIndex = getMessageDataIndex(body[0])
                body.extend([None] * (dataIndex + 1 - len(body)))
                body[dataIndex] = bytes(raw[4 + headerLength:])
    except Exception:
        return False
    return body if isinstance(body, list) and body and isNumber(body[0]) else False


class Listener:

    def __init__(self, callback, once):
        self.callback = callback
        self.once = once


class EventEmitter:

    def __init__(self):
        self._listenerMap = {}
        self._maxListeners = 10

    def on(self, name, callback, once=False):
        if not isinstance(name, str) or not callable(callback):
            return self
        listenerList = self._listenerMap.setdefault(name, [])
        listenerList.append(Listener(callback, once is True))
        if self._maxListeners > 0 and len(listenerList) > self._maxListeners:
            logger.warning('EventEmitter %s, exceeded maxListeners: %s/%s. Use setMaxListeners(n)', name, self._maxListeners, len(listenerList))
        return self

    def once(self, name, callback):
        return self.on(name, callback, True)

    def getListenerNameList(self):
        return list(self._listenerMap.keys())

    def clearAllListeners(self):
        self._listenerMap = {}
        return self._listenerMap

    def listenerCount(self, name):
        return len(self._listenerMap.get(name, []))

    def setMaxListeners(self, count):
        if not isNumber(count):
            return False
        self._maxListeners = int(count)
        return True

    def off(self, name, callback):
        listenerList = self._listenerMap.get(name)
        if not listenerList or not callable(callback):
            return self
        listenerList[:] = [listener for listener in listenerList if listener.callback != callback]
        if not listenerList:
            del self._listenerMap[name]
        return self

    def offAll(self, name):
        self._listenerMap.pop(name, None)
        return self

    def emit(self, name, *args):
        listenerList = self._listenerMap.get(name)
        if not listenerList:
            return self
        for listener in list(listenerList):
            currentList = self._listenerMap.get(name)
            if not currentList or not any(item is listener for item in currentList):
                continue
            if listener.once:
                currentList.remove(listener)
                if not currentList:
                    del self._listenerMap[name]
            result = listener.callback(*args)
            if inspect.isawaitable(result):
                promise(result)
        return self


class Connection:

    def __init__(self, websocket, query=None):
        self._websocket   = websocket
        self._query       = query if isinstance(query, dict) else {}
        self._timeOffset  = 0
        self._isClose     = False
        self._outputQueue = asyncio.Queue()
        self._writerTask  = promise(self.__writer())

    def getWebsocket(self):
        return self._websocket

    def getQuery(self):
        return self._query

    def setQuery(self, query):
        self._query = query if isinstance(query, dict) else {}

    def getTimeOffset(self):
        return self._timeOffset

    def setTimeOffset(self, timeOffset):
        self._timeOffset = timeOffset

    def isConnection(self):
        return not self._isClose and self._websocket.state is State.OPEN

    def sendMessage(self, msg):
        if not self.isConnection():
            return False
        self._outputQueue.put_nowait(('send', str(msg)))
        return True

    def sendObject(self, body):
        if not self.isConnection():
            return False
        try:
            message = encodeMessage(body)
        except Exception:
            return False
        self._outputQueue.put_nowait(('send', message))
        return True

    def closeConnection(self, msg='close'):
        if not self.isConnection():
            return False
        self._isClose = True
        reason = str(msg or 'close').encode()[:123].decode(errors='ignore')
        self._outputQueue.put_nowait(('close', reason))
        return True

    def stop(self):
        self._isClose = True
        self._writerTask.cancel()

    async def __writer(self):
        while True:
            action, payload = await self._outputQueue.get()
            try:
                if action == 'close':
                    return await self._websocket.close(1000, payload)
                await self._websocket.send(payload)
            except Exception:
                return
