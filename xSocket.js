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
xSocket.helpers = new function helpersObject (){

    /**
     *
     * @returns {Window|*}
     */
    this.getWindow = function (){
        return xSocket.window;
    };

    /**
     *
     * @returns {any}
     */
    this.getProcess = function (){
        return xSocket.process;
    };

    /**
     *
     * @param min
     * @param max
     * @returns {number}
     */
    this.getRandInt = function (min, max){
        return Math.floor(min + Math.random() * (max + 1 - min));
    };

    /**
     *
     * @param data
     * @returns {boolean}
     */
    var isBinary = this.isBinary = function (data){
        return typeof ArrayBuffer === 'function' && (data instanceof ArrayBuffer || ArrayBuffer.isView(data));
    };

    /**
     *
     * @param type
     * @returns {number}
     */
    var getMessageDataIndex = this.getMessageDataIndex = function (type){
        return type === xSocket.xSocketObject.MSG_SD_CREATE ? 4 : 2;
    };

    /**
     *
     * @param body
     * @returns {string|Uint8Array}
     */
    this.encodeMessage = function (body){
        var dataIndex = getMessageDataIndex(body[0]);
        var data = body[dataIndex];
        if(!isBinary(data)){
            return JSON.stringify(body);
        }
        body = body.slice();
        body[dataIndex] = null;
        var headerBytes = new TextEncoder().encode(JSON.stringify(body));
        var dataBytes = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
        var frame = new Uint8Array(4 + headerBytes.length + dataBytes.length);
        new DataView(frame.buffer).setUint32(0, headerBytes.length);
        frame.set(headerBytes, 4);
        frame.set(dataBytes, 4 + headerBytes.length);
        return frame;
    };

    /**
     *
     * @param raw
     * @returns {Array|boolean}
     */
    this.decodeMessage = function (raw){
        var body;
        try{
            if(typeof raw === 'string'){
                body = JSON.parse(raw);
            }else{
                var frame = raw instanceof ArrayBuffer ? new Uint8Array(raw) : raw;
                var headerLength = new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(0);
                body = JSON.parse(new TextDecoder().decode(frame.subarray(4, 4 + headerLength)));
                if(Array.isArray(body)){
                    body[getMessageDataIndex(body[0])] = frame.subarray(4 + headerLength);
                }
            }
        }catch (e){
            return false;
        }
        return Array.isArray(body) && typeof body[0] === 'number' ? body : false;
    };

    /**
     *
     * @constructor
     */
    var EventEmitter = this.EventEmitter = function EventEmitter(){
        this.__listenerMap = Object.create(null);
        this.__maxListeners = 10;
    };

    /**
     * Append EE listener
     * @param name
     * @param callback
     * @param once
     * @return {EventEmitter}
     */
    EventEmitter.prototype.on = function (name, callback, once){
        if(typeof name !== 'string' || typeof callback !== 'function'){
            return this;
        }
        var listenerList = this.__listenerMap[name] || (this.__listenerMap[name] = []);
        listenerList.push({'callback' : callback, 'once' : once === true});
        if(this.__maxListeners > 0 && listenerList.length > this.__maxListeners){
            console.warn('EventEmitter ', name, ', exceeded maxListeners : ', this.__maxListeners + '/' + listenerList.length, '. Use EE.setMaxListeners(n)!');
        }
        return this;
    };

    /**
     * Append once EE listener
     * @param name
     * @param callback
     * @return {EventEmitter}
     */
    EventEmitter.prototype.once = function (name, callback){
        return this.on(name, callback, true);
    };

    /**
     * Getter all listener name
     * @returns {string[]}
     */
    EventEmitter.prototype.getListerensName = function (){
        return Object.keys(this.__listenerMap);
    };

    /**
     * Clear all listener
     * @returns {{}}
     */
    EventEmitter.prototype.clearAllListerens = function (){
        this.__listenerMap = Object.create(null);
        return this.__listenerMap;
    };

    /**
     * Getter count EE listener
     * @param name
     * @returns {number}
     */
    EventEmitter.prototype.listenerCount = function (name){
        var listenerList = this.__listenerMap[name];
        return listenerList ? listenerList.length : 0;
    };

    /**
     * Set max listeners
     * @param count
     * @returns {boolean}
     */
    EventEmitter.prototype.setMaxListeners = function (count){
        if(typeof count !== 'number'){
            return false;
        }
        this.__maxListeners = parseInt(count);
        return true;
    };

    /**
     * Delete EE listener
     * @param name
     * @param callback
     * @return {EventEmitter}
     */
    EventEmitter.prototype.off = function (name, callback){
        var listenerList = this.__listenerMap[name];
        if(!listenerList || typeof callback !== 'function'){
            return this;
        }
        for(var i = listenerList.length - 1; i >= 0; i--){
            if(listenerList[i].callback === callback){
                listenerList.splice(i, 1);
            }
        }
        if(!listenerList.length){
            delete this.__listenerMap[name];
        }
        return this;
    };

    /**
     * Delete all EE listener
     * @param name
     * @return {EventEmitter}
     */
    EventEmitter.prototype.offAll = function (name){
        delete this.__listenerMap[name];
        return this;
    };

    /**
     * Emit EE listener
     * @param name
     * @param data_1
     * @param data_2
     * @param data_3
     * @param data_4
     * @param data_5
     * @return {EventEmitter}
     */
    EventEmitter.prototype.emit = function (name, data_1, data_2, data_3, data_4, data_5){
        var listenerList = this.__listenerMap[name];
        if(!listenerList){
            return this;
        }
        listenerList = listenerList.slice();
        for(var i = 0; i < listenerList.length; i++){
            var currentList = this.__listenerMap[name];
            var index = currentList ? currentList.indexOf(listenerList[i]) : -1;
            if(index === -1){
                continue;
            }
            if(listenerList[i].once){
                currentList.splice(index, 1);
                if(!currentList.length){
                    delete this.__listenerMap[name];
                }
            }
            listenerList[i].callback.call(this, data_1, data_2, data_3, data_4, data_5);
        }
        return this;
    };
};
xSocket.VERSION = '2.2.0';

/**
 *
 * @param __isOutput
 * @param __object
 * @param __socketObject
 * @constructor
 */
xSocket.Data = function xSocketData(__isOutput, __object, __socketObject){
    xSocket.helpers.EventEmitter.call(this);
    this.setMaxListeners(0);
    __object = typeof __object === 'object' && __object ? __object : {};

    var $this = this;
    this.__isOutput     = !!__isOutput;
    this.__socketObject = __socketObject || false;
    this.__ID           = typeof __object.ID === 'string' ? __object.ID : '';
    this.__type         = typeof __object.type === 'string' ? __object.type : '';
    this.__data         = typeof __object.data === 'object' ? __object.data : {};
    this.__ttl          = typeof __object.ttl === 'number' && __object.ttl % 1 === 0 && __object.ttl >= 0 ? __object.ttl : xSocket.Data.DEFAULT_TTL;
    var now             = this.__socketObject ? this.__socketObject.getTime() : Date.now();
    this.__time         = typeof __object.time === 'number' && __object.time > 0 ? __object.time : now;
    this.__resData      = {};
    this.__error        = false;
    this.__status       = xSocket.Data.STATUS_NEW;
    this.__destroy      = false;
    this.__ttlTimeout   = setTimeout(function (){
        $this.destroy('ttl', true);
    }, Math.max(0, Math.min(this.__ttl, this.__ttl - (now - this.__time))));
};
xSocket.Data.prototype = Object.create(xSocket.helpers.EventEmitter.prototype);
xSocket.Data.prototype.constructor = xSocket.Data;
xSocket.Data.STATUS_NEW      = 0;
xSocket.Data.STATUS_SEND     = 1;
xSocket.Data.STATUS_RESPONSE = 2;
xSocket.Data.DEFAULT_TTL     = 3000;

/**
 *
 * @returns {string}
 */
xSocket.Data.prototype.getID = function (){
    return String(this.__ID);
};

/**
 *
 * @returns {string}
 */
xSocket.Data.prototype.getType = function (){
    return String(this.__type);
};

/**
 *
 * @returns {number}
 */
xSocket.Data.prototype.getTTL = function (){
    return this.__ttl;
};

/**
 *
 * @returns {number}
 */
xSocket.Data.prototype.getTime = function (){
    return this.__time;
};

/**
 *
 * @returns {{}}
 */
xSocket.Data.prototype.getData = function (){
    return this.__data || {};
};

/**
 *
 * @param key
 * @param def
 * @returns {*}
 */
xSocket.Data.prototype.getDataVal = function (key, def){
    return this.getData()[key] || def;
};

/**
 *
 * @returns {{}}
 */
xSocket.Data.prototype.getResData = function (){
    return this.__resData || {};
};

/**
 *
 * @param key
 * @param def
 * @returns {*}
 */
xSocket.Data.prototype.getResDataVal = function (key, def){
    return this.getResData()[key] || def;
};

/**
 *
 * @returns {number}
 */
xSocket.Data.prototype.getStatus = function (){
    return this.__status;
};

/**
 *
 * @returns {boolean|string}
 */
xSocket.Data.prototype.getDestroy = function (){
    return this.__destroy || false;
};

/**
 *
 * @returns {xSocket.xSocketObject|boolean}
 */
xSocket.Data.prototype.getSocketObject = function (){
    return this.__socketObject;
};

/**
 *
 * @returns {string|boolean}
 */
xSocket.Data.prototype.getError = function (){
    return this.__error || false;
};

/**
 *
 * @returns {boolean}
 */
xSocket.Data.prototype.isNew = function (){
    return this.__status === xSocket.Data.STATUS_NEW;
};

/**
 *
 * @returns {boolean}
 */
xSocket.Data.prototype.isSend = function (){
    return this.__status === xSocket.Data.STATUS_SEND;
};

/**
 *
 * @returns {boolean}
 */
xSocket.Data.prototype.isResponse = function (){
    return this.__status === xSocket.Data.STATUS_RESPONSE;
};

/**
 *
 * @param data
 * @param error
 * @returns {Promise<xSocket.Data>}
 */
xSocket.Data.prototype.response = function (data, error){
    var $this = this;
    var promise = new Promise(function (resolve, reject){
        if($this.__isOutput){
            return reject(new Error('Output data'));
        }
        if($this.__destroy || $this.isResponse() || !$this.__ttl){
            return reject(new Error('destroy'));
        }
        $this.setResData(data);
        $this.setError(error);
        $this.once('response', function (){
            resolve($this);
        });
        $this.once('destroy', function (msg){
            reject(new Error(msg));
        });
        $this.__socketObject.getWsReady().then(function (ws){
            if(ws.sendObject([
                xSocket.xSocketObject.MSG_SD_UPDATE,
                Number($this.__ID),
                $this.getResData(),
                $this.getError()
            ])){
                $this.setStatus(xSocket.Data.STATUS_RESPONSE);
            }else{
                $this.destroy('Send failed');
            }
        }).catch(function (e){
            $this.destroy(e.message || 'Send failed');
        });
    });
    promise.catch(function (){});
    return promise;
};

/**
 *
 * @param status
 * @returns {boolean}
 */
xSocket.Data.prototype.setStatus = function (status){
    if(
        (status !== xSocket.Data.STATUS_SEND && status !== xSocket.Data.STATUS_RESPONSE) ||
        this.__status === xSocket.Data.STATUS_RESPONSE ||
        this.__status === status
    ){
        return false;
    }
    this.__status = status;
    this.emit('setStatus', status);
    if(status === xSocket.Data.STATUS_RESPONSE){
        this.emit('response', this.getResData(), this.getError());
        this.destroy('response');
    }
    return true;
};

/**
 *
 * @param errorMsg
 * @returns {boolean}
 */
xSocket.Data.prototype.setError = function (errorMsg){
    if(typeof errorMsg !== 'string'){
        return false;
    }
    this.__error = errorMsg;
    return true;
};

/**
 *
 * @param data
 * @returns {boolean}
 */
xSocket.Data.prototype.setResData = function (data){
    if(typeof data !== 'object'){
        return false;
    }
    this.__resData = data;
    return true;
};

/**
 *
 * @param msg
 * @param isSetError
 * @returns {boolean}
 */
xSocket.Data.prototype.destroy = function (msg, isSetError){
    if(this.__destroy){
        return false;
    }
    this.__destroy = typeof msg === 'string' && msg.length ? msg : 'destroy';
    clearTimeout(this.__ttlTimeout);
    if(isSetError && !this.__error){
        this.setError(this.__destroy);
    }
    if(this.__status !== xSocket.Data.STATUS_RESPONSE){
        this.setStatus(xSocket.Data.STATUS_RESPONSE);
    }
    this.emit('destroy', this.__destroy);
    this.clearAllListerens();
    return true;
};

/**
 *
 * @param __ServerConfigure
 * @param __req
 * @constructor
 */
xSocket.xSocketObject = function xSocketObject(__ServerConfigure, __req){
    xSocket.helpers.EventEmitter.call(this);
    this.setMaxListeners(0);

    this.__serverConfigure = __ServerConfigure;
    this.__req             = __req;
    this.__ws              = undefined;
    this.__ID              = '';
    this.__sign            = '';
    this.__destroy         = undefined;
    this.__isNewConnection = true;
    this.__socketDataMap   = {};
    this.__socketDataInc   = 0;
    this.__query           = {};
    this.__timeOffset      = 0;

    this.on('destroy', this.__onDestroy);
    this.on('ws|disconnect', this.__onWsDisconnect);
    this.on('ws|connect', this.__onWsConnect);
    this.on('ws|message', this.__onWsMessage);
};
xSocket.xSocketObject.prototype = Object.create(xSocket.helpers.EventEmitter.prototype);
xSocket.xSocketObject.prototype.constructor = xSocket.xSocketObject;
xSocket.xSocketObject.MSG_SO_UPDATE = 0;
xSocket.xSocketObject.MSG_SD_CREATE = 1;
xSocket.xSocketObject.MSG_SD_UPDATE = 2;
xSocket.xSocketObject.MSG_SO_TIME   = 3;

/**
 *
 * @returns {string}
 */
xSocket.xSocketObject.prototype.getID = function (){
    return this.__ID || '';
};

/**
 *
 * @returns {string}
 */
xSocket.xSocketObject.prototype.getSign = function (){
    return this.__sign || '';
};

/**
 *
 * @returns {number}
 */
xSocket.xSocketObject.prototype.getTime = function (){
    return Date.now() + this.__timeOffset;
};

/**
 *
 * @returns {number}
 */
xSocket.xSocketObject.prototype.getTimeOffset = function (){
    return this.__timeOffset;
};

/**
 *
 * @param timeOffset
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.setTimeOffset = function (timeOffset){
    if(typeof timeOffset !== 'number' || timeOffset !== timeOffset){
        return false;
    }
    this.__timeOffset = Math.round(timeOffset);
    return true;
};

/**
 *
 * @returns {*|{}}
 */
xSocket.xSocketObject.prototype.getReq = function (){
    return this.__req || {};
};

/**
 *
 * @returns {*}
 */
xSocket.xSocketObject.prototype.getQuery = function (){
    return typeof this.__query === 'object' ? this.__query : {};
};

/**
 *
 * @param key
 * @param def
 * @returns {*}
 */
xSocket.xSocketObject.prototype.getQueryVal = function (key, def){
    return this.getQuery()[key] || def;
};

/**
 *
 * @returns {*|{}}
 */
xSocket.xSocketObject.prototype.getReqHeaders = function (){
    return this.getReq()['headers'] || {};
};

/**
 *
 * @returns {*|string}
 */
xSocket.xSocketObject.prototype.getOrigin = function (){
    return this.getReqHeaders()['origin'] || 'unknown';
};

/**
 *
 * @returns {string}
 */
xSocket.xSocketObject.prototype.getIP = function (){
    return String((this.getReq()['connection'] || {})['remoteAddress'] || 'unknown');
};

/**
 *
 * @returns {{}}
 */
xSocket.xSocketObject.prototype.getSocketDataList = function (){
    return this.__socketDataMap;
};

/**
 *
 * @param ID
 * @returns {xSocket.Data|boolean}
 */
xSocket.xSocketObject.prototype.getSocketData = function (ID){
    return this.__socketDataMap[ID] || false;
};

/**
 *
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.isConnection = function (){
    return typeof this.__ws === 'object' ? this.__ws.isConnection() : false;
};

/**
 *
 * @returns {any|boolean}
 */
xSocket.xSocketObject.prototype.getConnection = function (){
    return this.isConnection() ? this.__ws : false;
};

/**
 *
 * @returns {string|boolean}
 */
xSocket.xSocketObject.prototype.getDestroy = function (){
    return this.__destroy || false;
};

/**
 *
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.isDestroy = function (){
    return !!this.getDestroy();
};

/**
 *
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.isServer = function (){
    return !!this.__serverConfigure;
};

/**
 *
 * @returns {Promise}
 */
xSocket.xSocketObject.prototype.getWsReady = function (){
    var $this = this;
    return new Promise(function (resolve, reject){
        if($this.getConnection()){
            return resolve($this.getConnection());
        }
        var destroyHandler = function (){
            $this.off('connect', connectHandler);
            reject(new Error('destroy'));
        };
        var connectHandler = function (){
            $this.off('destroy', destroyHandler);
            if($this.getConnection()){
                resolve($this.getConnection());
            }else{
                reject(new Error('Connection failed'));
            }
        };
        $this.once('connect', connectHandler);
        $this.once('destroy', destroyHandler);
    });
};

/**
 *
 * @param type
 * @param data
 * @param ttl
 * @returns {Promise<xSocket.Data>}
 */
xSocket.xSocketObject.prototype.send = function (type, data, ttl){
    var $this = this;
    var socketData = new xSocket.Data(true, {
        'ID' : String(++this.__socketDataInc),
        'type' : type,
        'data' : data,
        'ttl' : ttl,
        'time' : this.getTime()
    }, this);
    if(socketData.getTTL()){
        this.__socketDataMap[socketData.getID()] = socketData;
        socketData.once('destroy', function (){
            delete $this.__socketDataMap[socketData.getID()];
        });
    }
    this.getWsReady().then(function (ws){
        if(!ws.sendObject([
            xSocket.xSocketObject.MSG_SD_CREATE,
            Number(socketData.getID()),
            socketData.getType(),
            socketData.getTTL(),
            socketData.getData(),
            socketData.getTime()
        ])){
            return socketData.destroy('Send failed');
        }
        socketData.setStatus(socketData.getTTL() ? xSocket.Data.STATUS_SEND : xSocket.Data.STATUS_RESPONSE);
    }).catch(function (e){
        socketData.destroy(e.message || 'Send failed');
    });
    return Promise.resolve(socketData);
};

/**
 *
 * @param type
 * @param data
 * @param ttl
 * @returns {Promise<xSocket.Data>}
 */
xSocket.xSocketObject.prototype.sendReadyResponse = function (type, data, ttl){
    return this.send(type, data, ttl).then(function (socketData){
        return new Promise(function (resolve, reject){
            var responseHandler = function (){
                if(socketData.getError()){
                    return reject(new Error(socketData.getError()));
                }
                resolve(socketData);
            };
            if(socketData.isResponse() || socketData.getDestroy()){
                return responseHandler();
            }
            socketData.once('response', responseHandler);
        });
    });
};

/**
 *
 * @returns {Promise<xSocket.Data>}
 */
xSocket.xSocketObject.prototype.sendReady = xSocket.xSocketObject.prototype.sendReadyResponse;

/**
 *
 * @param msg
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.destroy = function (msg){
    if(this.isDestroy()){
        return false;
    }
    this.__destroy = String(msg || 'destroy');
    this.disconnect(this.__destroy);
    this.emit('destroy', this, this.__destroy);
    this.clearAllListerens();
    return true;
};

/**
 *
 * @param msg
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.disconnect = function (msg){
    var ws = this.getConnection();
    if(!ws){
        return false;
    }
    return ws.closeConnection(msg);
};

/**
 *
 * @param ws
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.update = function (ws){
    if(typeof ws !== 'object' || typeof ws.isConnection !== 'function' || typeof ws.query !== 'object'){
        return false;
    }
    if(!ws.isConnection() || this.__destroy){
        return false;
    }
    if(this.__ID !== ws.query['xSOId'] || this.__sign !== ws.query['xSOSign']){
        this.__ID   = ws.query['xSOId'];
        this.__sign = ws.query['xSOSign'];
        this.__query = Object(ws['query'] || {});
        delete this.__query['xSOId'];
        delete this.__query['xSOSign'];
        delete this.__query['xV'];
        this.emit('update', this);
    }
    if(this.isServer()){
        ws.sendObject([
            xSocket.xSocketObject.MSG_SO_UPDATE,
            this.getID(),
            this.getSign(),
            typeof this.__serverConfigure === 'object' ? this.__serverConfigure : {},
            xSocket.VERSION,
            Date.now()
        ]);
    }else{
        this.setTimeOffset(ws.timeOffset);
        ws.sendObject([xSocket.xSocketObject.MSG_SO_TIME, Date.now()]);
    }
    this.__ws = ws;
    ws.SocketObject = this;
    this.emit('ws|connect', ws);
    return true;
};

xSocket.xSocketObject.prototype.__onDestroy = function (){
    for(var ID in this.__socketDataMap){
        this.__socketDataMap[ID].destroy('SocketObject|destroy');
    }
};

xSocket.xSocketObject.prototype.__onWsDisconnect = function (msg){
    msg = typeof msg === 'string' ? msg : 'unknown';
    this.emit('disconnect', this, msg);
    if(msg === 'end'){
        this.destroy('end');
    }
};

xSocket.xSocketObject.prototype.__onWsConnect = function (){
    this.emit('connect', this);
    if(!this.__isNewConnection){
        this.emit('reconnect', this);
    }else{
        this.__isNewConnection = false;
    }
};

xSocket.xSocketObject.prototype.__onWsMessage = function (body){
    if(body[0] === xSocket.xSocketObject.MSG_SO_TIME){
        return this.__onWsTime(body);
    }
    var ID = body[1];
    if(typeof ID !== 'number' || ID % 1 !== 0 || ID < 1){
        return;
    }
    var socketData;
    if(body[0] === xSocket.xSocketObject.MSG_SD_CREATE){
        if(typeof body[2] !== 'string' || !body[2].length || typeof body[4] !== 'object'){
            return;
        }
        socketData = new xSocket.Data(false, {
            'ID' : String(ID),
            'type' : body[2],
            'ttl' : body[3],
            'data' : body[4],
            'time' : body[5]
        }, this);
        this.emit('data', socketData);
        this.emit('data|' + socketData.getType(), socketData);
        if(!socketData.getTTL()){
            socketData.setStatus(xSocket.Data.STATUS_RESPONSE);
        }
    }else if(body[0] === xSocket.xSocketObject.MSG_SD_UPDATE){
        socketData = this.getSocketData(String(ID));
        if(socketData){
            socketData.setResData(body[2]);
            socketData.setError(body[3] || false);
            socketData.setStatus(xSocket.Data.STATUS_RESPONSE);
        }
    }
};

xSocket.xSocketObject.prototype.__onWsTime = function (body){
    var ws = this.getConnection();
    if(!ws || typeof body[1] !== 'number'){
        return;
    }
    if(this.isServer()){
        ws.sendObject([xSocket.xSocketObject.MSG_SO_TIME, body[1], Date.now()]);
    }else if(typeof body[2] === 'number'){
        var now = Date.now();
        this.setTimeOffset(body[2] + (now - body[1]) / 2 - now);
    }
};

xSocket.Client = function xSocketClient(serverUrl, __query, __settings){

    // Extend
    xSocket.helpers.EventEmitter.call(this);
    this.setMaxListeners(0);

    var $this = this;
    var __urlList = [];
    var __ws;
    var __SocketObject = false;
    var __isVersionWarned = false;


    /**
     *
     * @returns {{}}
     */
    this.getSettings = function(){
        return __settings || {};
    };

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    this.getSettingsVal = function (key, def){
        return this.getSettings()[key] || def;
    };

    /**
     *
     * @returns {xSocket.xSocketObject}
     */
    this.getSocketObject = function (){
        return __SocketObject;
    };


    /**
     *
     * @returns {Promise<xSocket.xSocketObject>}
     */
    this.getSocketObjectReady = function (){
        return new Promise(function (resolve) {
            if($this.getSocketObject()) return resolve($this.getSocketObject());
            $this.once('connect', function (socketObject) {
                resolve(socketObject);
            });
        });
    };

    /**
     *
     * @returns {Promise<{}>}
     */
    this.getWs = function (){
        return new Promise(function (resolve){
            if(typeof __ws === 'object'){
                return resolve(__ws);
            }
            if(__ws === null){
                return $this.once('ws|connect', function (ws){
                    resolve(ws);
                });
            }
            __ws = null;
            var url = __urlList[xSocket.helpers.getRandInt(0, __urlList.length - 1)];
            url += (url.indexOf('?') === -1 ? '?' : '&');

            __query['xV'] = xSocket.VERSION;
            if(__SocketObject && __SocketObject.getID().length && __SocketObject.getSign().length){
                __query['xSOId'] = String(__SocketObject.getID());
                __query['xSOSign'] = String(__SocketObject.getSign());
            }

            if(Object.keys(__query).length){
                for (var i in __query){
                    try{
                        url += encodeURIComponent(String(i))+'='+encodeURIComponent(String(__query[i]))+'&';
                    }catch (e){}
                }
            }

            var ws = new xSocket.WebSocket(url);
            ws.binaryType = 'arraybuffer';
            ws.closeConnection = function (msg){
                if(ws.isConnection()){
                    try{
                        ws.close(1000, String(msg || 'close'));
                        return true;
                    }catch (e){
                        console.error(e);
                    }
                }
                return false;
            };
            ws.isConnection = function (){
                return ws.readyState === 1;
            };
            ws.sendMessage = function (msg){
                try{
                    if(this.isConnection()){
                        ws.send(String(msg));
                        return true;
                    }
                }catch (e){}
                return false;
            };
            ws.sendObject = function(object) {
                try {
                    if(ws.isConnection()){
                        ws.send(xSocket.helpers.encodeMessage(object));
                        return true;
                    }
                }catch (e){}
                return false;
            };
            var msg;
            var msgInc = 0;
            var pongTimeout = false;
            ws.onopen = function (){
                __ws = ws;
                $this.emit('ws|connect', ws);
                pongTimeout = setInterval(function (){
                    if(!msgInc){
                        ws.closeConnection('pingTimeout');
                    }
                    msgInc = 0;
                }, 21000);
                resolve(__ws);
            }
            ws.onmessage = function (e){
                msgInc++;
                if(e.data === 'ping'){
                    return ws.sendMessage('pong');
                }
                try {
                    var body = xSocket.helpers.decodeMessage(e.data);
                    if (!body) {
                        return;
                    }
                    if (body[0] === xSocket.xSocketObject.MSG_SO_UPDATE) {
                        if(body[4] !== xSocket.VERSION && !__isVersionWarned){
                            __isVersionWarned = true;
                            console.warn('[warn|xSocket]', 'Version mismatch', 'client ' + xSocket.VERSION + ', server ' + (body[4] || 'unknown'));
                        }
                        ws.timeOffset = typeof body[5] === 'number' ? body[5] - Date.now() : 0;
                        ws.query = {
                            'xSOId' : String(body[1]),
                            'xSOSign' : String(body[2]),
                            'configure' : typeof body[3] === 'object' ? body[3] : {}
                        };
                        if(__SocketObject){
                            if(ws.query['xSOId'] === __SocketObject.getID() && ws.query['xSOSign'] === __SocketObject.getSign() && __SocketObject.update(ws)){
                                return false;
                            }
                            __SocketObject.destroy('update');
                            __SocketObject = undefined;
                        }
                        __SocketObject = new xSocket.xSocketObject();
                        __SocketObject.once('connect', function() {
                            $this.emit('connect', __SocketObject);
                        });
                        __SocketObject.on('destroy', function(socketObject, msg) {
                            __SocketObject = false;
                            $this.emit('destroy', socketObject, msg);
                        });
                        __SocketObject.on('disconnect', function(socketObject, msg) {
                            $this.emit('disconnect', socketObject, msg);
                        });
                        __SocketObject.on('reconnect', function() {
                            $this.emit('reconnect', __SocketObject);
                        });

                        __SocketObject.on('data', function(socketData) {
                            $this.emit('data', socketData);
                        });
                        if(!__SocketObject.update(ws)){
                            ws.closeConnection('update');
                        }
                    }else{
                        if(__SocketObject){
                            __SocketObject.emit('ws|message', body);
                        }
                    }
                }catch (e){}
            };
            ws.onclose = function (e){
                if(pongTimeout) try{ clearInterval(pongTimeout); }catch (e){}

                __ws = undefined;
                msg = msg || e.reason || 'close';
                $this.emit('ws|disconnect', msg);
                if(__SocketObject){
                    __SocketObject.emit('ws|disconnect', msg);
                }
            };
            ws.onerror = function (e){
                msg = e.message;
                $this.emit('ws|error', e.message, ws);
            };
        });
    };

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<xSocket.Data>}
     */
    this.send = function (type, data, ttl){
        return $this.getSocketObjectReady().then(function (socketObject){
            return socketObject.send(type, data, ttl);
        });
    };

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<xSocket.Data>}
     */
    this.sendReady = function (type, data, ttl){
        return $this.getSocketObjectReady().then(function (socketObject){
            return socketObject.sendReady(type, data, ttl);
        });
    };

    /**
     *
     * @returns {Promise<xSocket.Data>}
     */
    this.sendReadyResponse = this.sendReady;

    /**
     *
     * @param msg
     * @returns {*}
     */
    this.disconnect = function (msg){
        var socketObject = this.getSocketObject();
        if(!socketObject) return false;
        return socketObject.disconnect(msg);
    };

    /**
     * Construct
     */
    (function (){
        if(typeof serverUrl === "object"){
            for(var i in serverUrl){
                __urlList.push(serverUrl[i]);
            }
        }else if(typeof serverUrl === "string"){
            __urlList.push(serverUrl);
        }
        if(!__urlList.length){
            throw new Error('Invalid urlList');
        }
        __settings = typeof __settings === 'object' ? __settings : {};
        __query     = typeof __query === 'object' ? __query : {};
        $this.on('ws|disconnect', function(){
            setTimeout(function(){
                    $this.getWs()['catch'](function (){});
            }, 1000);
        });
        $this.on('ws|error', function(){
            setTimeout(function(){
                $this.getWs()['catch'](function (){});
            }, 1000);
        });
        if(xSocket.helpers.getWindow()){
            var closeConnection = function() {
                try{
                    __ws.closeConnection('end');
                }catch (e){}
            };
            xSocket.helpers.getWindow().addEventListener("pagehide", closeConnection);
            xSocket.helpers.getWindow().addEventListener("unload", closeConnection);
        }else if(xSocket.helpers.getProcess()){
            xSocket.helpers.getProcess().on('exit', function (){
                try{
                    __ws.closeConnection('end');
                }catch (e){}
            });
        }
        $this.on('connect', function (socketObject){
            var destroyTimeout = false;
            var setDestroyTimeout = function (action, ttl){
                if(destroyTimeout){
                    try{
                        clearTimeout(destroyTimeout);
                        destroyTimeout = false;
                    }catch (e){}
                }
                if(action){
                    ttl = typeof ttl === 'number' && !isNaN(ttl) && ttl > 0 ? ttl : 0;
                    destroyTimeout = setTimeout(function (socketObject){
                        socketObject.destroy('socketObjectTimeout');
                        destroyTimeout = false;
                    }, ttl, socketObject);
                }
            }

            socketObject.on('reconnect', function (){
                setDestroyTimeout(false);
            });
            socketObject.on('disconnect', function (){
                var socketObjectTimeout = parseInt(Object(socketObject.getQueryVal('configure', {}))['socketObjectTimeout'] || 0);
                if(typeof socketObjectTimeout === 'number'){
                    setDestroyTimeout(true, socketObjectTimeout);
                }
            });
            socketObject.once('destroy', function (){
                setDestroyTimeout(false);
            });
        });
        $this.getWs().catch(function (){});
    })();

};
xSocket.Client.prototype = Object.create(xSocket.helpers.EventEmitter.prototype);
xSocket.Client.prototype.constructor = xSocket.Client;
